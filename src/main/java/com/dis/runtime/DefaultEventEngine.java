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

// 默认事件引擎实现，是整个事件流的“总装配器”和“生命周期控制器”。
// 核心流程：
// 1. 构造阶段创建 RingBuffer 和 Sequencer，RingBuffer 存事件槽位，Sequencer 分配和发布序号。
// 2. start 之前通过 handleEventsWith / then 组装 pipeline，每个 stage 都会注册自己的消费序号。
// 3. 发布阶段先申请序号，再写入对应槽位，最后发布序号唤醒消费者。
// 4. 运行阶段由 BatchEventProcessor 或 WorkProcessor 消费事件，并通过 observer 回写指标。
// 5. 停机阶段分为立即停止和优雅停止，优雅停止会等消费者追上当前 cursor 后再关闭线程。
public final class DefaultEventEngine<E> implements EventEngine<E> {
    private final RingBuffer<EventSlot<E>> ringBuffer; // 环形缓冲区，生产者和消费者都按 sequence 定位槽位。
    private final MultiProducerSequencer sequencer; // 多生产者序号分配器，同时负责发布可见性和背压。
    private final EngineConfig<E> config; // 引擎配置，保存工厂、线程、等待策略、异常策略、重试策略和观测开关。
    private final List<StageRuntime<E>> runtimes = new ArrayList<>(); // 每个 stage 的运行时对象，start / halt / 指标采集都会遍历它。
    private final AtomicReference<EngineState> state = new AtomicReference<>(EngineState.NEW); // 引擎状态机。
    // 对外暴露的发布入口很薄，只把同步发布和带超时发布统一转到内部发布流程。
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
    private volatile CountDownLatch shutdownLatch = new CountDownLatch(0); // 等待所有处理线程退出；start 时按实际线程数重建。
    private final EngineMetricsCollector metricsCollector = new EngineMetricsCollector(); // 记录发布、重试、死信和 stage 进度。
    private final EngineHealthEvaluator healthEvaluator = new EngineHealthEvaluator(); // 根据指标快照计算健康状态。
    private final ObservabilityLogger observabilityLogger = new ObservabilityLogger(); // 输出周期性观测日志。
    // 观测日志使用单独的 daemon 线程，避免阻塞事件处理主流程。
    private final ScheduledExecutorService observabilityScheduler =
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread reporter = new Thread(runnable, "dis-observability-reporter");
                reporter.setDaemon(true);
                return reporter;
            });
    private volatile long startNanos; // 引擎启动时间，用于计算 uptime。
    private final AtomicInteger stageNoGenerator = new AtomicInteger(1); // 未指定名称时生成普通 stage 编号。
    private final AtomicInteger workerPoolNoGenerator = new AtomicInteger(1); // 未指定名称时生成 worker stage 编号。

    public DefaultEventEngine(EngineConfig<E> config) {
        this.config = Objects.requireNonNull(config, "config");
        // 每个槽位只创建一次，后续发布时复用槽位里的 event，减少对象分配。
        this.ringBuffer = new RingBuffer<>(config.bufferSize(), () -> new EventSlot<>(config.eventFactory().get()));
        // bufferSize 和 waitStrategy 必须与 RingBuffer 保持一致，生产者和消费者才会在同一套序号体系下协作。
        this.sequencer = new MultiProducerSequencer(
                config.bufferSize(),
                config.waitStrategy(),
                config.publishSignalPolicy()
        );
    }

    public static <E> DefaultEventEngine<E> create(EngineConfig<E> config) {
        return new DefaultEventEngine<>(config);
    }

    //单事件被handle
    @SafeVarargs
    @Override
    public final EventChain<E> handleEventsWith(String stageName, BusinessEventHandler<E>... handlers) {
        // pipeline 只能在启动前搭建，启动后再改依赖关系会破坏消费者进度和背压判断。
        ensureNotStarted();
        if (handlers == null || handlers.length == 0) {
            throw new IllegalArgumentException("处理器不能为空");
        }
        // 第一层普通 stage 没有上游依赖，所有 handler 都直接等待生产者发布的 sequence。
        int stageNo = stageNoGenerator.getAndIncrement();
        Sequence[] stageSequences = appendBatchStage(stageName, null, handlers, stageNo);
        // 返回 EventChain 让调用方继续 then，把本 stage 的 sequence 作为下一层依赖。
        return new EventChainImpl(stageSequences);
    }

    //单事件只会被一个handle处理
    @Override
    public EventChain<E> handleEventsWithWorkerPool(String stageName, int workerCount, WorkHandler<E> handler) {
        // worker pool 也是 pipeline 的一部分，同样必须在 start 前完成注册。
        ensureNotStarted();
        if (workerCount <= 0) {
            throw new IllegalArgumentException("工作线程数量必须大于 0");
        }
        Objects.requireNonNull(handler, "handler");
        // 第一层 worker pool 没有上游依赖，多个 worker 竞争同一批生产者发布的 sequence。
        Sequence[] workerSequences = appendWorkerPoolStage(stageName, null, workerCount, handler);
        return new EventChainImpl(workerSequences);
    }

    @Override
    public EventPublisher<E> publisher() {
        return publisher;
    }

    @Override
    public void start() {
        // 核心方法：启动引擎并拉起所有 stage 处理线程。
        // 关键点：状态机从 NEW->STARTED、计算线程总数、初始化 shutdownLatch、按 runtime 启动线程。
        // 仅允许 NEW -> STARTED，防止重复启动或关闭后重启。
        if (!state.compareAndSet(EngineState.NEW, EngineState.STARTED)) {
            throw new IllegalStateException("引擎已经启动或已经关闭");
        }

        startNanos = System.nanoTime();
        int totalThreads = 0;
        for (StageRuntime<E> runtime : runtimes) {
            // 普通 stage 是 1 个线程，worker pool 是 workerCount 个线程。
            totalThreads += runtime.threadCount();
        }
        // latch 的计数必须等于实际处理线程数，awaitShutdown / graceful shutdown 才能准确等待退出。
        shutdownLatch = new CountDownLatch(totalThreads);

        ThreadFactory threadFactory = config.threadFactory();
        for (StageRuntime<E> runtime : runtimes) {
            // 每个 runtime 自己知道如何启动对应的 processor，并在退出时 countDown。
            runtime.start(threadFactory, shutdownLatch);
        }

        if (config.observabilityLogEnabled()) {
            long interval = config.observabilityLogIntervalSeconds();
            // 定时输出健康报告，异常会在 logPeriodicObservability 内部吞掉，避免影响引擎运行。
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
        // 核心方法：快速停机。
        // 关键点：不等待 drain，直接设置 SHUTDOWN 并 halt/interrupt 各处理线程。
        // 快速停机：直接打断处理线程，不保证 drain。
        if (state.get() == EngineState.SHUTDOWN) {
            return;
        }

        // 这里不等待消费者追平 cursor，适合进程退出或调用方已经能接受未处理事件的场景。
        state.set(EngineState.SHUTDOWN);
        observabilityScheduler.shutdownNow();
        for (StageRuntime<E> runtime : runtimes) {
            // halt 会先通知 processor 停止，再 interrupt 阻塞在等待策略里的线程。
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
        // 核心方法：优雅停机。
        // 关键点：先进入 DRAINING，拒绝新发布；等待所有消费者追平当前 cursor；再停止线程并等待退出。
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0) {
            throw new IllegalArgumentException("超时时间必须大于等于 0");
        }

        // 小粒度时间单位换算成纳秒可能溢出或被截断，这里把正数但换算后非正的情况视为无限等待。
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
            // 引擎还没启动时没有处理线程需要 drain，直接关闭观测线程即可。
            state.set(EngineState.SHUTDOWN);
            observabilityScheduler.shutdownNow();
            return true;
        }
        if (current == EngineState.STARTED && !state.compareAndSet(EngineState.STARTED, EngineState.DRAINING)) {
            // 如果并发线程已经改了状态，就重新读取一次，按最新状态继续判断。
            current = state.get();
        }
        if (current != EngineState.STARTED && current != EngineState.DRAINING) {
            return state.get() == EngineState.SHUTDOWN;
        }

        // DRAINING 期间不允许新发布，只等待“当前 cursor”被所有 stage 追平。
        while (true) {
            // cursor 表示当前已经发布到的最大 sequence。
            long currentCursor = sequencer.cursorSequence().getVolatile();
            while (minConsumerSequence(currentCursor) < currentCursor) {
                long elapsed = System.nanoTime() - startTime;
                if (elapsed >= timeoutNanos) {
                    metricsCollector.recordGracefulShutdownTimeout();
                    return false;
                }
                long remaining = timeoutNanos - elapsed;
                // 用短暂 park 轮询消费者进度，避免忙等把 CPU 打满。
                LockSupport.parkNanos(Math.min(remaining, 1_000_000L));
            }

            // 防止等待期间又有新序号发布，重新校验一轮。
            if (sequencer.cursorSequence().getVolatile() == currentCursor) {
                break;
            }
        }

        for (StageRuntime<E> runtime : runtimes) {
            // 追平后再停止处理器，保证已发布事件尽量被完整消费。
            runtime.halt();
        }

        long elapsed = System.nanoTime() - startTime;
        if (elapsed >= timeoutNanos) {
            metricsCollector.recordGracefulShutdownTimeout();
            return false;
        }

        long remainingNanos = timeoutNanos - elapsed;
        // 等待每个处理线程从 run() 中退出，避免方法返回时后台线程仍在工作。
        if (!shutdownLatch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
            metricsCollector.recordGracefulShutdownTimeout();
            return false;
        }

        // 所有处理线程都退出后，状态才最终进入 SHUTDOWN。
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
        // cursor 和最慢消费者序号一起决定积压量，是健康检查和观测日志的基础数据。
        long cursor = sequencer.cursorSequence().getVolatile();
        long minConsumerSequence = minConsumerSequence(cursor);
        long uptimeMillis = startNanos == 0
                ? 0
                : TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        List<EngineMetricsCollector.StageRuntimeView> runtimeViews = new ArrayList<>();
        for (StageRuntime<E> runtime : runtimes) {
            // 普通 stage 返回一个 view，worker pool 会按 worker 拆成多个 view。
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
        // 核心方法：计算当前最慢消费者进度。
        // 作用：用于积压量计算、优雅停机追平判断、生产者覆盖保护相关观测。
        long min = Long.MAX_VALUE;
        for (StageRuntime<E> runtime : runtimes) {
            // 取所有 runtime 中最慢的 sequence，用它判断生产者是否还能覆盖旧槽位。
            min = Math.min(min, runtime.minSequence());
        }
        // 没有任何消费者时返回 fallback，避免 Long.MAX_VALUE 影响积压量计算。
        return min == Long.MAX_VALUE ? fallback : min;
    }

    private void ensureNotStarted() {
        if (state.get() != EngineState.NEW) {
            throw new IllegalStateException("流水线只能在启动前配置");
        }
    }

    private void logPeriodicObservability() {
        try {
            observabilityLogger.logPeriodic(healthReport());
        } catch (Throwable error) {
            System.err.println("观测日志输出失败：" + error.getMessage());
        }
    }

    private void publishInternal(EventTranslator<E> translator) {
        // 核心方法：阻塞发布入口。
        // 语义：无限等待可用槽位，直到发布完成；若引擎不接收事件则抛异常。
        // 普通 publish 语义是一直等待可用槽位；如果引擎不接收事件则抛异常。
        if (!tryPublishInternal(translator, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
            throw new IllegalStateException("引擎当前不接收事件");
        }
    }

    private boolean tryPublishInternal(EventTranslator<E> translator, long timeout, TimeUnit unit) {
        // 核心方法：带超时发布。
        // 主流程：claim sequence -> reset+translate -> 标记 READY/TRANSLATE_FAILED -> publish sequence。
        // 注意：无论 translate 成功或失败都必须 publish，避免序号空洞卡死后续消费。
        Objects.requireNonNull(translator, "translator");
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0) {
            throw new IllegalArgumentException("超时时间必须大于等于 0");
        }
        if (state.get() != EngineState.STARTED) {
            throw new IllegalStateException("发布事件前必须先启动引擎");
        }

        long start = System.nanoTime();
        // 先向 Sequencer 申请一个独占 sequence；申请不到说明缓冲区被慢消费者顶住了。
        long sequence = timeout == Long.MAX_VALUE
                ? sequencer.next()
                : sequencer.tryNext(timeout, unit);
        if (sequence < 0) {
            metricsCollector.recordPublishTimeout();
            return false;
        }

        // sequence 和 RingBuffer 下标是一一映射关系，生产者拿到 sequence 后才能写对应槽位。
        EventSlot<E> slot = ringBuffer.get(sequence);
        EventResetter<E> eventResetter = config.eventResetter();
        try {
            // reset + translate 保证槽位复用安全。
            slot.resetForTranslate(sequence);
            // 先清理旧事件对象，再让业务 translator 写入本次发布的数据。
            eventResetter.reset(slot.event());
            translator.translateTo(slot.event(), sequence);
            // markReady 是消费者判断该槽位可正常处理的信号。
            slot.markReady();
            metricsCollector.recordPublishSuccess(System.nanoTime() - start);
            return true;
        } catch (Throwable ex) {
            // translator 失败也要占位并发布失败态，避免 sequence 空洞。
            slot.markTranslateFailed(ex);
            metricsCollector.recordPublishTranslateFailed(System.nanoTime() - start);
            throw ex;
        } finally {
            // 无论写入成功还是失败，都必须 publish；否则后续 sequence 会被这个空洞卡住。
            sequencer.publish(sequence);
        }
    }

    private Sequence[] appendBatchStage(String stageNamePrefix,
                                        Sequence dependency,
                                        BusinessEventHandler<E>[] handlers,
                                        int stageNo) {
        // 核心方法：追加广播型 Batch stage。给runtimes添加处理器
        // 关键点：每个 handler 对应一个独立 BatchEventProcessor；每个 processor 的 sequence 都注册为 gating sequence。
        // batch stage 是广播模式：同一层有几个 handler，就有几个 processor 各自完整消费每个事件。
        Sequence[] stageSequences = new Sequence[handlers.length];
        String baseStageName = stageNamePrefix == null || stageNamePrefix.isBlank()
                ? "阶段-" + stageNo
                : stageNamePrefix;

        for (int i = 0; i < handlers.length; i++) {
            String stageName = baseStageName + "-handler-" + (i + 1);
            // 每个 handler 单独注册指标，后续能看到每条消费链路自己的延迟、错误和进度。
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
            // gating sequence 告诉生产者：这个消费者没处理完的槽位不能被覆盖。
            sequencer.addGatingSequence(sequence);
            stageSequences[i] = sequence;
            // runtime 延迟到 start 时再真正拉起线程，配置阶段只保存结构。
            runtimes.add(new BatchStageRuntime(stageName, processor, sequence));
        }

        return stageSequences;
    }

    private Sequence[] appendWorkerPoolStage(String stageNamePrefix,
                                             Sequence dependency,
                                             int workerCount,
                                             WorkHandler<E> handler) {
        // 核心方法：追加竞争消费型 WorkerPool stage。
        // 关键点：多个 worker 共享 workSequence 抢任务；每个 worker 的私有 sequence 都注册为 gating sequence。
        // worker pool 是竞争消费模式：同一个事件只会被其中一个 worker 处理。
        // shared workSequence 是 worker 之间抢任务的公共游标，用来决定下一个 sequence 分配给谁。
        Sequence workSequence = new Sequence(-1);
        Sequence[] workerSequences = new Sequence[workerCount];
        List<WorkProcessor<E>> processors = new ArrayList<>(workerCount);
        String baseStageName = stageNamePrefix == null || stageNamePrefix.isBlank()
                ? "工作池阶段-" + workerPoolNoGenerator.getAndIncrement()
                : stageNamePrefix;

        for (int i = 0; i < workerCount; i++) {
            String workerName = baseStageName + "-工作线程-" + (i + 1);
            StageMetrics stageMetrics = metricsCollector.registerStage(workerName);
            // 每个 worker 有自己的消费进度，但共享同一个 workSequence 来避免重复处理同一事件。
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
            // 每个 worker 的 sequence 都要加入 gating，生产者必须等最慢 worker 释放槽位。
            workerSequences[i] = processor.getSequence();
            sequencer.addGatingSequence(workerSequences[i]);
            processors.add(processor);
        }

        runtimes.add(new WorkerPoolRuntime(baseStageName, processors, workerSequences));
        return workerSequences;
    }

    private final class EventChainImpl implements EventChain<E> {
        private Sequence[] dependencies; // 当前链路最后一层的消费进度，继续 then 时会变成下一层依赖。

        private EventChainImpl(Sequence[] dependencies) {
            this.dependencies = dependencies;
        }

        @Override
        public EventChain<E> then(String stageName, BusinessEventHandler<E>... handlers) {
            ensureNotStarted();
            if (handlers == null || handlers.length == 0) {
                throw new IllegalArgumentException("处理器不能为空");
            }
            // 如果上一层只有一个消费者，直接依赖它；如果上一层是多个消费者，用 SequenceGroup 表示“全部完成”。
            Sequence dependency = dependencies.length == 1
                    ? dependencies[0]
                    : new SequenceGroup(dependencies);

            int stageNo = stageNoGenerator.getAndIncrement();
            // 新增的 batch stage 会等待上游 dependency 达到目标 sequence 后才处理事件。
            dependencies = appendBatchStage(stageName, dependency, handlers, stageNo);
            return this;
        }

        @Override
        public EventChain<E> thenWorkerPool(String stageName, int workerCount, WorkHandler<E> handler) {
            ensureNotStarted();
            if (workerCount <= 0) {
                throw new IllegalArgumentException("工作线程数量必须大于 0");
            }
            Objects.requireNonNull(handler, "handler");

            // worker pool 接到链路后面时，也必须等上一层所有消费者处理完同一个 sequence。
            Sequence dependency = dependencies.length == 1
                    ? dependencies[0]
                    : new SequenceGroup(dependencies);

            dependencies = appendWorkerPoolStage(stageName, dependency, workerCount, handler);
            return this;
        }
    }

    // 把不同类型 stage 的启动、停止、进度和观测视图统一抽象出来，主流程就不用关心具体 processor 类型。
    private interface StageRuntime<T> {
        int threadCount();

        void start(ThreadFactory threadFactory, CountDownLatch shutdownLatch);

        void halt();

        long minSequence();

        List<EngineMetricsCollector.StageRuntimeView> views();
    }

    private final class BatchStageRuntime implements StageRuntime<E> {
        private final String stageName; // stage 名称，用于指标视图。
        private final BatchEventProcessor<E> processor; // 普通广播消费者。
        private final Sequence sequence; // 当前 stage 的消费进度。
        private volatile Thread workerThread; // 实际运行 processor 的线程。

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
            // 普通 stage 一个 processor 对应一个线程，线程退出时释放 shutdownLatch。
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
            // 先设置 processor 的停止标记，再 interrupt，兼容阻塞等待和自旋等待两类策略。
            processor.halt();
            Thread worker = workerThread;
            if (worker != null) {
                worker.interrupt();
            }
        }

        @Override
        public long minSequence() {
            // 普通 stage 只有一个消费者，自己的 sequence 就是该 runtime 的消费进度。
            return sequence.getVolatile();
        }

        @Override
        public List<EngineMetricsCollector.StageRuntimeView> views() {
            Thread worker = workerThread;
            // view 同时包含消费进度和线程存活状态，健康检查可以据此判断卡顿或线程退出。
            return List.of(new EngineMetricsCollector.StageRuntimeView(
                    stageName,
                    sequence.getVolatile(),
                    worker != null && worker.isAlive()
            ));
        }
    }

    private final class WorkerPoolRuntime implements StageRuntime<E> {
        private final String stageName; // worker pool 名称。
        private final List<WorkProcessor<E>> processors; // pool 内所有 worker processor。
        private final Sequence[] workerSequences; // 每个 worker 自己的消费进度。
        private final List<Thread> workerThreads = new ArrayList<>(); // 每个 worker 对应的运行线程。

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
                // worker pool 中每个 WorkProcessor 各占一个线程，共同竞争 workSequence。
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
            // 先通知所有 worker 停止，再统一 interrupt，避免部分线程继续抢任务。
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
                // worker pool 释放槽位时看最慢 worker，防止某个 worker 尚未完成的槽位被覆盖。
                min = Math.min(min, workerSequence.getVolatile());
            }
            return min == Long.MAX_VALUE ? -1 : min;
        }

        @Override
        public List<EngineMetricsCollector.StageRuntimeView> views() {
            List<EngineMetricsCollector.StageRuntimeView> views = new ArrayList<>(processors.size());
            for (int i = 0; i < processors.size(); i++) {
                Thread worker = i < workerThreads.size() ? workerThreads.get(i) : null;
                // 每个 worker 单独输出进度和线程状态，方便定位是哪一个 worker 慢或已经退出。
                views.add(new EngineMetricsCollector.StageRuntimeView(
                        stageName + "-工作线程-" + (i + 1),
                        workerSequences[i].getVolatile(),
                        worker != null && worker.isAlive()
                ));
            }
            return views;
        }
    }

    private final class StageProcessingObserver implements ProcessingObserver<E> {
        private final StageMetrics stageMetrics; // 当前 stage 的指标容器。

        private StageProcessingObserver(StageMetrics stageMetrics) {
            this.stageMetrics = stageMetrics;
        }

        @Override
        public void onSuccess(long sequence, E event, long latencyNanos) {
            // handler 正常完成时，记录当前 stage 的成功次数和处理耗时。
            stageMetrics.recordSuccess(sequence, latencyNanos);
        }

        @Override
        public void onFailure(long sequence, E event, Throwable cause, long latencyNanos) {
            // handler 最终失败但没有进入死信时，记录错误信息用于健康报告和日志。
            stageMetrics.recordError(sequence, latencyNanos, cause);
        }

        @Override
        public void onRetry(long sequence, E event, Throwable cause, int attempt, long latencyNanos) {
            // 每次重试既记录 stage 维度，也记录引擎全局维度，方便看单点和整体的重试压力。
            stageMetrics.recordRetry(sequence, latencyNanos, cause);
            metricsCollector.recordHandlerRetry();
        }

        @Override
        public void onDeadLetter(long sequence, E event, Throwable cause, int attempts, long latencyNanos) {
            // 重试耗尽后进入死信，说明事件没有被业务 handler 成功处理。
            stageMetrics.recordDeadLetter(sequence, latencyNanos, cause);
            metricsCollector.recordDeadLetter();
        }

        @Override
        public void onSkippedPublishFailure(long sequence, E event, Throwable cause) {
            // 如果生产者 translate 阶段失败，消费者会跳过该槽位，并把跳过次数纳入指标。
            stageMetrics.recordSkippedPublishFailure(sequence, cause);
            metricsCollector.recordConsumerSkippedTranslateFailed();
        }
    }
}
