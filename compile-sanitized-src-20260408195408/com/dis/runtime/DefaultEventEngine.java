package com.dis.runtime;

import com.dis.api.BusinessEventHandler;
import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.api.EventChain;
import com.dis.api.EventEngine;
import com.dis.api.EventPublisher;
import com.dis.api.EventTranslator;
import com.dis.core.BatchEventProcessor;
import com.dis.core.MultiProducerSequencer;
import com.dis.core.RingBuffer;
import com.dis.core.Sequence;
import com.dis.handler.EventHandler;
import com.dis.runtime.observability.EngineHealthEvaluator;
import com.dis.runtime.observability.EngineMetricsCollector;
import com.dis.runtime.observability.ObservabilityLogger;
import com.dis.runtime.observability.StageMetrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 默认事件引擎实现。
 *
 * 目标：
 * 1. 对业务提供稳定、简洁的接入 API。
 * 2. 复用底层高性能并发组件。
 * 3. 内建指标、健康与周期日志能力。
 */
public final class DefaultEventEngine<E> implements EventEngine<E> {
    private final RingBuffer<E> ringBuffer;
    private final MultiProducerSequencer sequencer;
    private final EngineConfig<E> config;

    // 每个处理器的运行时信息（阶段名、处理器、序号、线程）。
    private final List<ProcessorRuntime<E>> runtimes = new ArrayList<>();

    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.NEW);
    private final EventPublisher<E> publisher = this::publishInternal;
    private volatile CountDownLatch shutdownLatch = new CountDownLatch(0);

    // 可观测性组件。
    private final EngineMetricsCollector metricsCollector = new EngineMetricsCollector();
    private final EngineHealthEvaluator healthEvaluator = new EngineHealthEvaluator();
    private final ObservabilityLogger observabilityLogger = new ObservabilityLogger();

    // 周期日志调度器，使用守护线程，避免阻塞进程退出。
    private final ScheduledExecutorService observabilityScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread reporter = new Thread(runnable, "dis-observability-reporter");
                reporter.setDaemon(true);
                return reporter;
            });

    private volatile long startNanos;
    private final AtomicInteger stageNoGenerator = new AtomicInteger(1);

    public DefaultEventEngine(EngineConfig<E> config) {
        this.config = Objects.requireNonNull(config, "config");
        this.ringBuffer = new RingBuffer<>(config.bufferSize(), config.eventFactory());
        this.sequencer = new MultiProducerSequencer(config.bufferSize(), config.waitStrategy());
    }

    public static <E> DefaultEventEngine<E> create(EngineConfig<E> config) {
        return new DefaultEventEngine<>(config);
    }

    @SafeVarargs
    @Override
    public final EventChain<E> handleEventsWith(BusinessEventHandler<E>... handlers) {
        // 这里只做处理链注册，不会立即启动线程处理数据。
        ensureNotStarted();
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers must not be empty");
        }

        int stageNo = stageNoGenerator.getAndIncrement();
        Sequence[] stageSequences = appendStage(null, handlers, stageNo);
        return new EventChainImpl(stageSequences);
    }

    @Override
    public EventPublisher<E> publisher() {
        return publisher;
    }

    @Override
    public void start() {
        if (!state.compareAndSet(EngineState.NEW, EngineState.STARTED)) {
            throw new IllegalStateException("engine already started or shutdown");
        }

        startNanos = System.nanoTime();
        shutdownLatch = new CountDownLatch(runtimes.size());

        for (ProcessorRuntime<E> runtime : runtimes) {
            Thread worker = config.threadFactory().newThread(() -> {
                try {
                    runtime.processor.run();
                } finally {
                    shutdownLatch.countDown();
                }
            });
            runtime.workerThread = worker;
            worker.start();
        }

        // 开启周期观测日志。
        if (config.observabilityLogEnabled()) {
            long interval = config.observabilityLogIntervalSeconds();
            observabilityScheduler.scheduleAtFixedRate(
                    this::logPeriodicObservability,
                    interval,
                    interval,
                    TimeUnit.SECONDS
            );
        }
    }

    @Override
    public void shutdown() {
        if (state.get() == EngineState.SHUTDOWN) {
            return;
        }

        state.set(EngineState.SHUTDOWN);
        observabilityScheduler.shutdownNow();

        // 先下发停止信号。
        for (ProcessorRuntime<E> runtime : runtimes) {
            runtime.processor.halt();
        }

        // 再中断线程，确保阻塞等待可以及时退出。
        for (ProcessorRuntime<E> runtime : runtimes) {
            Thread worker = runtime.workerThread;
            if (worker != null) {
                worker.interrupt();
            }
        }
    }

    @Override
    public boolean awaitShutdown(long timeout, TimeUnit unit) throws InterruptedException {
        return shutdownLatch.await(timeout, unit);
    }

    @Override
    public EngineMetricsSnapshot metricsSnapshot() {
        long cursor = sequencer.cursorSequence().getVolatile();
        long minConsumerSequence = minConsumerSequence(cursor);
        long uptimeMillis = startNanos == 0
                ? 0
                : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        List<EngineMetricsCollector.StageRuntimeView> runtimeViews = new ArrayList<>(runtimes.size());
        for (ProcessorRuntime<E> runtime : runtimes) {
            Thread worker = runtime.workerThread;
            boolean alive = worker != null && worker.isAlive();
            runtimeViews.add(new EngineMetricsCollector.StageRuntimeView(
                    runtime.stageName,
                    runtime.sequence.getVolatile(),
                    alive
            ));
        }

        return metricsCollector.snapshot(
                state.get(),
                uptimeMillis,
                cursor,
                minConsumerSequence,
                config.bufferSize(),
                runtimeViews
        );
    }

    @Override
    public EngineHealthReport healthReport() {
        return healthEvaluator.evaluate(metricsSnapshot(), config);
    }

    private void publishInternal(EventTranslator<E> translator) {
        if (state.get() != EngineState.STARTED) {
            throw new IllegalStateException("engine must be started before publishing events");
        }

        long start = System.nanoTime();
        long sequence = sequencer.next();
        try {
            E event = ringBuffer.get(sequence);
            translator.translateTo(event, sequence);
            metricsCollector.recordPublishSuccess(System.nanoTime() - start);
        } catch (Throwable ex) {
            metricsCollector.recordPublishError(System.nanoTime() - start);
            throw ex;
        } finally {
            sequencer.publish(sequence);
        }
    }

    private Sequence[] appendStage(Sequence dependency,
                                   BusinessEventHandler<E>[] handlers,
                                   int stageNo) {
        Sequence[] stageSequences = new Sequence[handlers.length];

        for (int i = 0; i < handlers.length; i++) {
            String stageName = "stage-" + stageNo + "-handler-" + (i + 1);
            StageMetrics stageMetrics = metricsCollector.registerStage(stageName);
            EventHandler<E> observed = observedHandler(handlers[i], stageMetrics);

            BatchEventProcessor<E> processor = new BatchEventProcessor<>(
                    ringBuffer,
                    sequencer,
                    dependency,
                    config.waitStrategy(),
                    observed,
                    config.exceptionHandler()
            );

            Sequence sequence = processor.getSequence();
            sequencer.addGatingSequence(sequence);

            stageSequences[i] = sequence;
            runtimes.add(new ProcessorRuntime<>(stageName, processor, sequence));
        }

        return stageSequences;
    }

    private EventHandler<E> observedHandler(BusinessEventHandler<E> businessHandler,
                                            StageMetrics stageMetrics) {
        return (event, sequence) -> {
            long start = System.nanoTime();
            try {
                businessHandler.onEvent(event, sequence);
                stageMetrics.recordSuccess(sequence, System.nanoTime() - start);
            } catch (Throwable ex) {
                stageMetrics.recordError(sequence, System.nanoTime() - start);
                throw ex;
            }
        };
    }

    private long minConsumerSequence(long fallback) {
        long min = Long.MAX_VALUE;
        for (ProcessorRuntime<E> runtime : runtimes) {
            min = Math.min(min, runtime.sequence.getVolatile());
        }
        return min == Long.MAX_VALUE ? fallback : min;
    }

    private void ensureNotStarted() {
        if (state.get() != EngineState.NEW) {
            throw new IllegalStateException("pipeline can only be configured before start");
        }
    }

    private void logPeriodicObservability() {
        try {
            observabilityLogger.logPeriodic(healthReport());
        } catch (Throwable error) {
            // 观测日志异常不能影响业务线程。
            System.err.println("观测日志输出失败: " + error.getMessage());
        }
    }

    private final class EventChainImpl implements EventChain<E> {
        private Sequence[] dependencies;

        private EventChainImpl(Sequence[] dependencies) {
            this.dependencies = dependencies;
        }

        @SafeVarargs
        @Override
        public final EventChain<E> then(BusinessEventHandler<E>... handlers) {
            // 在当前阶段后追加下一阶段。
            ensureNotStarted();
            if (handlers == null || handlers.length == 0) {
                throw new IllegalArgumentException("handlers must not be empty");
            }

            Sequence dependency = dependencies.length == 1
                    ? dependencies[0]
                    : new SequenceGroup(dependencies);

            int stageNo = stageNoGenerator.getAndIncrement();
            dependencies = appendStage(dependency, handlers, stageNo);
            return this;
        }
    }

    private static final class ProcessorRuntime<E> {
        private final String stageName;
        private final BatchEventProcessor<E> processor;
        private final Sequence sequence;
        private volatile Thread workerThread;

        private ProcessorRuntime(String stageName, BatchEventProcessor<E> processor, Sequence sequence) {
            this.stageName = stageName;
            this.processor = processor;
            this.sequence = sequence;
        }
    }
}
