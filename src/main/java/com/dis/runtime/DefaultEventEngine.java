package com.dis.runtime;

import com.dis.api.BusinessEventHandler;
import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.api.EventChain;
import com.dis.api.EventEngine;
import com.dis.api.EventPublisher;
import com.dis.api.EventResetter;
import com.dis.api.EventTranslator;
import com.dis.core.BatchEventProcessor;
import com.dis.core.EventSlot;
import com.dis.core.MultiProducerSequencer;
import com.dis.core.ProcessingObserver;
import com.dis.core.RingBuffer;
import com.dis.core.Sequence;
import com.dis.core.Sequencer;
import com.dis.core.WorkProcessor;
import com.dis.handler.WorkHandler;
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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * 默认事件引擎实现。
 */
public final class DefaultEventEngine<E> implements EventEngine<E> {
    private final RingBuffer<EventSlot<E>> ringBuffer;
    private final MultiProducerSequencer sequencer;
    private final EngineConfig<E> config;
    private final List<StageRuntime<E>> runtimes = new ArrayList<>();
    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.NEW);
    private final EventPublisher<E> publisher = new EventPublisher<>() {
        @Override
        public void publishEvent(EventTranslator<E> translator) {
            publishInternal(translator);
        }

        @Override
        public boolean tryPublishEvent(EventTranslator<E> translator, long timeout, TimeUnit unit) {
            return tryPublishInternal(translator, timeout, unit);
        }
    };
    private volatile CountDownLatch shutdownLatch = new CountDownLatch(0);
    private final EngineMetricsCollector metricsCollector = new EngineMetricsCollector();
    private final EngineHealthEvaluator healthEvaluator = new EngineHealthEvaluator();
    private final ObservabilityLogger observabilityLogger = new ObservabilityLogger();
    private final ScheduledExecutorService observabilityScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread reporter = new Thread(runnable, "dis-observability-reporter");
                reporter.setDaemon(true);
                return reporter;
            });
    private volatile long startNanos;
    private final AtomicInteger stageNoGenerator = new AtomicInteger(1);
    private final AtomicInteger workerPoolNoGenerator = new AtomicInteger(1);

    public DefaultEventEngine(EngineConfig<E> config) {
        this.config = Objects.requireNonNull(config, "config");
        this.ringBuffer = new RingBuffer<>(config.bufferSize(), () -> new EventSlot<>(config.eventFactory().get()));
        this.sequencer = new MultiProducerSequencer(config.bufferSize(), config.waitStrategy());
    }

    public static <E> DefaultEventEngine<E> create(EngineConfig<E> config) {
        return new DefaultEventEngine<>(config);
    }

    @SafeVarargs
    @Override
    public final EventChain<E> handleEventsWith(String stageName, BusinessEventHandler<E>... handlers) {
        ensureNotStarted();
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("handlers must not be empty");
        }
        int stageNo = stageNoGenerator.getAndIncrement();
        Sequence[] stageSequences = appendBatchStage(stageName, null, handlers, stageNo);
        return new EventChainImpl(stageSequences);
    }

    @Override
    public EventChain<E> handleEventsWithWorkerPool(String stageName, int workerCount, WorkHandler<E> handler) {
        ensureNotStarted();
        if (workerCount <= 0) {
            throw new IllegalArgumentException("workerCount must be > 0");
        }
        Objects.requireNonNull(handler, "handler");
        Sequence[] workerSequences = appendWorkerPoolStage(stageName, null, workerCount, handler);
        return new EventChainImpl(workerSequences);
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
        int totalThreads = 0;
        for (StageRuntime<E> runtime : runtimes) {
            totalThreads += runtime.threadCount();
        }
        shutdownLatch = new CountDownLatch(totalThreads);

        ThreadFactory threadFactory = config.threadFactory();
        for (StageRuntime<E> runtime : runtimes) {
            runtime.start(threadFactory, shutdownLatch);
        }

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
        for (StageRuntime<E> runtime : runtimes) {
            runtime.halt();
        }
    }

    @Override
    public void shutdownGracefully() {
        try {
            shutdownGracefully(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean shutdownGracefully(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }

        long timeoutNanos = unit.toNanos(timeout);
        if (timeout > 0 && timeoutNanos <= 0L) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long startTime = System.nanoTime();

        EngineState current = state.get();
        if (current == EngineState.SHUTDOWN) {
            return true;
        }
        if (current == EngineState.NEW) {
            state.set(EngineState.SHUTDOWN);
            observabilityScheduler.shutdownNow();
            return true;
        }
        if (current == EngineState.STARTED && !state.compareAndSet(EngineState.STARTED, EngineState.DRAINING)) {
            current = state.get();
        }
        if (current != EngineState.STARTED && current != EngineState.DRAINING) {
            return state.get() == EngineState.SHUTDOWN;
        }

        while (true) {
            long currentCursor = sequencer.cursorSequence().getVolatile();
            while (minConsumerSequence(currentCursor) < currentCursor) {
                long elapsed = System.nanoTime() - startTime;
                if (elapsed >= timeoutNanos) {
                    metricsCollector.recordGracefulShutdownTimeout();
                    return false;
                }
                long remaining = timeoutNanos - elapsed;
                LockSupport.parkNanos(Math.min(remaining, 1_000_000L));
            }

            if (sequencer.cursorSequence().getVolatile() == currentCursor) {
                break;
            }
        }

        for (StageRuntime<E> runtime : runtimes) {
            runtime.halt();
        }

        long elapsed = System.nanoTime() - startTime;
        if (elapsed >= timeoutNanos) {
            metricsCollector.recordGracefulShutdownTimeout();
            return false;
        }

        long remainingNanos = timeoutNanos - elapsed;
        if (!shutdownLatch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
            metricsCollector.recordGracefulShutdownTimeout();
            return false;
        }

        state.set(EngineState.SHUTDOWN);
        observabilityScheduler.shutdownNow();
        metricsCollector.recordGracefulShutdown();
        return true;
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

        List<EngineMetricsCollector.StageRuntimeView> runtimeViews = new ArrayList<>();
        for (StageRuntime<E> runtime : runtimes) {
            runtimeViews.addAll(runtime.views());
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

    private long minConsumerSequence(long fallback) {
        long min = Long.MAX_VALUE;
        for (StageRuntime<E> runtime : runtimes) {
            min = Math.min(min, runtime.minSequence());
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
            System.err.println("观测日志输出失败: " + error.getMessage());
        }
    }

    private void publishInternal(EventTranslator<E> translator) {
        if (!tryPublishInternal(translator, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            throw new IllegalStateException("engine is not accepting events");
        }
    }

    private boolean tryPublishInternal(EventTranslator<E> translator, long timeout, TimeUnit unit) {
        Objects.requireNonNull(translator, "translator");
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        if (state.get() != EngineState.STARTED) {
            throw new IllegalStateException("engine must be started before publishing events");
        }

        long start = System.nanoTime();
        long sequence = timeout == Long.MAX_VALUE
                ? sequencer.next()
                : sequencer.tryNext(timeout, unit);
        if (sequence < 0) {
            metricsCollector.recordPublishTimeout();
            return false;
        }

        EventSlot<E> slot = ringBuffer.get(sequence);
        EventResetter<E> eventResetter = config.eventResetter();
        try {
            slot.resetForTranslate(sequence);
            eventResetter.reset(slot.event());
            translator.translateTo(slot.event(), sequence);
            slot.markReady();
            metricsCollector.recordPublishSuccess(System.nanoTime() - start);
            return true;
        } catch (Throwable ex) {
            slot.markTranslateFailed(ex);
            metricsCollector.recordPublishTranslateFailed(System.nanoTime() - start);
            throw ex;
        } finally {
            sequencer.publish(sequence);
        }
    }

    private Sequence[] appendBatchStage(String stageNamePrefix,
                                        Sequence dependency,
                                        BusinessEventHandler<E>[] handlers,
                                        int stageNo) {
        Sequence[] stageSequences = new Sequence[handlers.length];
        String baseStageName = stageNamePrefix == null || stageNamePrefix.isBlank()
                ? "stage-" + stageNo
                : stageNamePrefix;

        for (int i = 0; i < handlers.length; i++) {
            String stageName = baseStageName + "-handler-" + (i + 1);
            StageMetrics stageMetrics = metricsCollector.registerStage(stageName);

            BatchEventProcessor<E> processor = new BatchEventProcessor<>(
                    ringBuffer,
                    sequencer,
                    dependency,
                    config.waitStrategy(),
                    new HandlerAdapter<>(handlers[i]),
                    config.exceptionHandler(),
                    config.retryPolicy(),
                    config.deadLetterHandler(),
                    stageName,
                    new StageProcessingObserver(stageMetrics)
            );

            Sequence sequence = processor.getSequence();
            sequencer.addGatingSequence(sequence);
            stageSequences[i] = sequence;
            runtimes.add(new BatchStageRuntime(stageName, processor, sequence));
        }

        return stageSequences;
    }

    private Sequence[] appendWorkerPoolStage(String stageNamePrefix,
                                             Sequence dependency,
                                             int workerCount,
                                             WorkHandler<E> handler) {
        Sequence workSequence = new Sequence(-1);
        Sequence[] workerSequences = new Sequence[workerCount];
        List<WorkProcessor<E>> processors = new ArrayList<>(workerCount);
        String baseStageName = stageNamePrefix == null || stageNamePrefix.isBlank()
                ? "worker-stage-" + workerPoolNoGenerator.getAndIncrement()
                : stageNamePrefix;

        for (int i = 0; i < workerCount; i++) {
            String workerName = baseStageName + "-worker-" + (i + 1);
            StageMetrics stageMetrics = metricsCollector.registerStage(workerName);
            WorkProcessor<E> processor = new WorkProcessor<>(
                    ringBuffer,
                    sequencer,
                    workSequence,
                    dependency,
                    config.waitStrategy(),
                    handler,
                    config.exceptionHandler(),
                    config.retryPolicy(),
                    config.deadLetterHandler(),
                    workerName,
                    new StageProcessingObserver(stageMetrics)
            );
            workerSequences[i] = processor.getSequence();
            sequencer.addGatingSequence(workerSequences[i]);
            processors.add(processor);
        }

        runtimes.add(new WorkerPoolRuntime(baseStageName, processors, workerSequences));
        return workerSequences;
    }

    private final class EventChainImpl implements EventChain<E> {
        private Sequence[] dependencies;

        private EventChainImpl(Sequence[] dependencies) {
            this.dependencies = dependencies;
        }

        @Override
        public EventChain<E> then(String stageName, BusinessEventHandler<E>... handlers) {
            ensureNotStarted();
            if (handlers == null || handlers.length == 0) {
                throw new IllegalArgumentException("handlers must not be empty");
            }

            Sequence dependency = dependencies.length == 1
                    ? dependencies[0]
                    : new SequenceGroup(dependencies);

            int stageNo = stageNoGenerator.getAndIncrement();
            dependencies = appendBatchStage(stageName, dependency, handlers, stageNo);
            return this;
        }

        @Override
        public EventChain<E> thenWorkerPool(String stageName, int workerCount, WorkHandler<E> handler) {
            ensureNotStarted();
            if (workerCount <= 0) {
                throw new IllegalArgumentException("workerCount must be > 0");
            }
            Objects.requireNonNull(handler, "handler");

            Sequence dependency = dependencies.length == 1
                    ? dependencies[0]
                    : new SequenceGroup(dependencies);

            dependencies = appendWorkerPoolStage(stageName, dependency, workerCount, handler);
            return this;
        }
    }

    private interface StageRuntime<T> {
        int threadCount();

        void start(ThreadFactory threadFactory, CountDownLatch shutdownLatch);

        void halt();

        long minSequence();

        List<EngineMetricsCollector.StageRuntimeView> views();
    }

    private final class BatchStageRuntime implements StageRuntime<E> {
        private final String stageName;
        private final BatchEventProcessor<E> processor;
        private final Sequence sequence;
        private volatile Thread workerThread;

        private BatchStageRuntime(String stageName, BatchEventProcessor<E> processor, Sequence sequence) {
            this.stageName = stageName;
            this.processor = processor;
            this.sequence = sequence;
        }

        @Override
        public int threadCount() {
            return 1;
        }

        @Override
        public void start(ThreadFactory threadFactory, CountDownLatch shutdownLatch) {
            Thread worker = threadFactory.newThread(() -> {
                try {
                    processor.run();
                } finally {
                    shutdownLatch.countDown();
                }
            });
            this.workerThread = worker;
            worker.start();
        }

        @Override
        public void halt() {
            processor.halt();
            Thread worker = workerThread;
            if (worker != null) {
                worker.interrupt();
            }
        }

        @Override
        public long minSequence() {
            return sequence.getVolatile();
        }

        @Override
        public List<EngineMetricsCollector.StageRuntimeView> views() {
            Thread worker = workerThread;
            return List.of(new EngineMetricsCollector.StageRuntimeView(
                    stageName,
                    sequence.getVolatile(),
                    worker != null && worker.isAlive()
            ));
        }
    }

    private final class WorkerPoolRuntime implements StageRuntime<E> {
        private final String stageName;
        private final List<WorkProcessor<E>> processors;
        private final Sequence[] workerSequences;
        private final List<Thread> workerThreads = new ArrayList<>();

        private WorkerPoolRuntime(String stageName, List<WorkProcessor<E>> processors, Sequence[] workerSequences) {
            this.stageName = stageName;
            this.processors = processors;
            this.workerSequences = workerSequences;
        }

        @Override
        public int threadCount() {
            return processors.size();
        }

        @Override
        public void start(ThreadFactory threadFactory, CountDownLatch shutdownLatch) {
            for (WorkProcessor<E> processor : processors) {
                Thread worker = threadFactory.newThread(() -> {
                    try {
                        processor.run();
                    } finally {
                        shutdownLatch.countDown();
                    }
                });
                workerThreads.add(worker);
                worker.start();
            }
        }

        @Override
        public void halt() {
            for (WorkProcessor<E> processor : processors) {
                processor.halt();
            }
            for (Thread worker : workerThreads) {
                worker.interrupt();
            }
        }

        @Override
        public long minSequence() {
            long min = Long.MAX_VALUE;
            for (Sequence workerSequence : workerSequences) {
                min = Math.min(min, workerSequence.getVolatile());
            }
            return min == Long.MAX_VALUE ? -1 : min;
        }

        @Override
        public List<EngineMetricsCollector.StageRuntimeView> views() {
            List<EngineMetricsCollector.StageRuntimeView> views = new ArrayList<>(processors.size());
            for (int i = 0; i < processors.size(); i++) {
                Thread worker = i < workerThreads.size() ? workerThreads.get(i) : null;
                views.add(new EngineMetricsCollector.StageRuntimeView(
                        stageName + "-worker-" + (i + 1),
                        workerSequences[i].getVolatile(),
                        worker != null && worker.isAlive()
                ));
            }
            return views;
        }
    }

    private final class StageProcessingObserver implements ProcessingObserver<E> {
        private final StageMetrics stageMetrics;

        private StageProcessingObserver(StageMetrics stageMetrics) {
            this.stageMetrics = stageMetrics;
        }

        @Override
        public void onSuccess(long sequence, E event, long latencyNanos) {
            stageMetrics.recordSuccess(sequence, latencyNanos);
        }

        @Override
        public void onFailure(long sequence, E event, Throwable cause, long latencyNanos) {
            stageMetrics.recordError(sequence, latencyNanos, cause);
        }

        @Override
        public void onRetry(long sequence, E event, Throwable cause, int attempt, long latencyNanos) {
            stageMetrics.recordRetry(sequence, latencyNanos, cause);
            metricsCollector.recordHandlerRetry();
        }

        @Override
        public void onDeadLetter(long sequence, E event, Throwable cause, int attempts, long latencyNanos) {
            stageMetrics.recordDeadLetter(sequence, latencyNanos, cause);
            metricsCollector.recordDeadLetter();
        }

        @Override
        public void onSkippedPublishFailure(long sequence, E event, Throwable cause) {
            stageMetrics.recordSkippedPublishFailure(sequence, cause);
            metricsCollector.recordConsumerSkippedTranslateFailed();
        }
    }
}
