package com.dis.runtime;

import com.dis.api.DeadLetterEvent;
import com.dis.api.DeadLetterHandler;
import com.dis.api.EventResetter;
import com.dis.api.RetryPolicy;
import com.dis.handler.ExceptionHandler;
import com.dis.handler.FatalExceptionHandler;
import com.dis.strategy.AlwaysSignalPolicy;
import com.dis.strategy.PublishSignalPolicy;
import com.dis.strategy.BlockingWaitStrategy;
import com.dis.strategy.WaitStrategy;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

// 引擎不可变配置。
// 核心逻辑：
// 1. DefaultEventEngine 启动后会被多个线程并发读取，所以 build 后不再允许修改。
// 2. Builder 统一完成参数校验和默认值填充，避免构造函数参数过多。
// 3. 这里把等待策略、异常处理、重试、死信和观测阈值集中起来，保持引擎主流程干净。
public final class EngineConfig<E> {
    private final int bufferSize; // RingBuffer 容量，必须是 2 的幂。
    private final Supplier<E> eventFactory; // 用于预创建每个槽位里的业务事件对象。
    private final WaitStrategy waitStrategy; // 生产者和消费者使用的等待策略。
    private final PublishSignalPolicy publishSignalPolicy; // publish 后的唤醒策略。
    private final ExceptionHandler<E> exceptionHandler; // 业务异常和停机异常处理器。
    private final ThreadFactory threadFactory; // 处理线程工厂。
    private final EventResetter<E> eventResetter; // 槽位复用前的清理逻辑，避免上一轮数据污染下一轮发布。
    private final RetryPolicy retryPolicy; // handler 失败后的重试策略。
    private final DeadLetterHandler<E> deadLetterHandler; // 重试耗尽后的死信处理器。

    // 可观测性配置。
    private final boolean observabilityLogEnabled; // 是否定时输出观测日志。
    private final long observabilityLogIntervalSeconds; // 观测日志输出间隔。
    private final double degradedBacklogRatio; // 健康状态降级的积压比例阈值。
    private final double downBacklogRatio; // 健康状态不可用的积压比例阈值。

    private EngineConfig(Builder<E> builder) {
        this.bufferSize = builder.bufferSize;
        this.eventFactory = Objects.requireNonNull(builder.eventFactory, "eventFactory");
        // 没有显式配置时使用保守默认值，保证最小配置也能直接启动。
        this.waitStrategy = Objects.requireNonNullElseGet(builder.waitStrategy, BlockingWaitStrategy::new);
        this.publishSignalPolicy = Objects.requireNonNullElseGet(builder.publishSignalPolicy, AlwaysSignalPolicy::new);
        this.exceptionHandler = Objects.requireNonNullElseGet(builder.exceptionHandler, FatalExceptionHandler::new);
        this.threadFactory = Objects.requireNonNullElseGet(builder.threadFactory, DefaultThreadFactory::new);
        this.eventResetter = Objects.requireNonNullElseGet(builder.eventResetter, () -> event -> {
        });
        this.retryPolicy = Objects.requireNonNullElseGet(builder.retryPolicy, RetryPolicy::noRetry);
        this.deadLetterHandler = Objects.requireNonNullElseGet(builder.deadLetterHandler, DefaultDeadLetterHandler::new);
        this.observabilityLogEnabled = builder.observabilityLogEnabled;
        this.observabilityLogIntervalSeconds = builder.observabilityLogIntervalSeconds;
        this.degradedBacklogRatio = builder.degradedBacklogRatio;
        this.downBacklogRatio = builder.downBacklogRatio;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public Supplier<E> eventFactory() {
        return eventFactory;
    }

    public WaitStrategy waitStrategy() {
        return waitStrategy;
    }

    public PublishSignalPolicy publishSignalPolicy() {
        return publishSignalPolicy;
    }

    public ExceptionHandler<E> exceptionHandler() {
        return exceptionHandler;
    }

    public ThreadFactory threadFactory() {
        return threadFactory;
    }

    public EventResetter<E> eventResetter() {
        return eventResetter;
    }

    public RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public DeadLetterHandler<E> deadLetterHandler() {
        return deadLetterHandler;
    }

    public boolean observabilityLogEnabled() {
        return observabilityLogEnabled;
    }

    public long observabilityLogIntervalSeconds() {
        return observabilityLogIntervalSeconds;
    }

    public double degradedBacklogRatio() {
        return degradedBacklogRatio;
    }

    public double downBacklogRatio() {
        return downBacklogRatio;
    }

    public static <E> Builder<E> builder(Supplier<E> eventFactory) {
        return new Builder<>(eventFactory);
    }

    public static final class Builder<E> {
        private int bufferSize = 1024; // 默认 RingBuffer 容量。
        private final Supplier<E> eventFactory; // 必填：槽位事件工厂。
        private WaitStrategy waitStrategy; // 可选：等待策略。
        private PublishSignalPolicy publishSignalPolicy; // 可选：发布唤醒策略。
        private ExceptionHandler<E> exceptionHandler; // 可选：异常处理器。
        private ThreadFactory threadFactory; // 可选：线程工厂。
        private EventResetter<E> eventResetter; // 可选：事件复用前的清理器。
        private RetryPolicy retryPolicy; // 可选：重试策略。
        private DeadLetterHandler<E> deadLetterHandler; // 可选：死信处理器。

        private boolean observabilityLogEnabled = true; // 默认开启观测日志。
        private long observabilityLogIntervalSeconds = 15; // 默认 15 秒输出一次观测日志。
        private double degradedBacklogRatio = 0.70; // 默认 70% 积压时降级。
        private double downBacklogRatio = 0.95; // 默认 95% 积压时不可用。

        private Builder(Supplier<E> eventFactory) {
            // eventFactory 是唯一强制参数，因为 RingBuffer 初始化必须依赖它预创建槽位事件。
            this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory");
        }

        public Builder<E> bufferSize(int bufferSize) {
            if (Integer.bitCount(bufferSize) != 1) {
                throw new IllegalArgumentException("缓冲区大小必须是 2 的幂");
            }
            // 2 的幂是 RingBuffer 使用位运算取模的前提。
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder<E> waitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
            return this;
        }

        public Builder<E> publishSignalPolicy(PublishSignalPolicy publishSignalPolicy) {
            this.publishSignalPolicy = publishSignalPolicy;
            return this;
        }

        public Builder<E> exceptionHandler(ExceptionHandler<E> exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        public Builder<E> threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder<E> eventResetter(EventResetter<E> eventResetter) {
            this.eventResetter = eventResetter;
            return this;
        }

        public Builder<E> retryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder<E> deadLetterHandler(DeadLetterHandler<E> deadLetterHandler) {
            this.deadLetterHandler = deadLetterHandler;
            return this;
        }

        public Builder<E> observabilityLogEnabled(boolean observabilityLogEnabled) {
            this.observabilityLogEnabled = observabilityLogEnabled;
            return this;
        }

        public Builder<E> observabilityLogIntervalSeconds(long observabilityLogIntervalSeconds) {
            if (observabilityLogIntervalSeconds <= 0) {
                throw new IllegalArgumentException("观测日志间隔秒数必须大于 0");
            }
            this.observabilityLogIntervalSeconds = observabilityLogIntervalSeconds;
            return this;
        }

        public Builder<E> degradedBacklogRatio(double degradedBacklogRatio) {
            if (degradedBacklogRatio <= 0 || degradedBacklogRatio >= 1) {
                throw new IllegalArgumentException("降级积压比例必须在 (0,1) 范围内");
            }
            // 积压达到该比例后健康状态会降级，但仍认为引擎可用。
            this.degradedBacklogRatio = degradedBacklogRatio;
            return this;
        }

        public Builder<E> downBacklogRatio(double downBacklogRatio) {
            if (downBacklogRatio <= 0 || downBacklogRatio > 1) {
                throw new IllegalArgumentException("不可用积压比例必须在 (0,1] 范围内");
            }
            // 积压达到该比例后健康状态会被判断为不可用。
            this.downBacklogRatio = downBacklogRatio;
            return this;
        }

        public EngineConfig<E> build() {
            if (downBacklogRatio <= degradedBacklogRatio) {
                throw new IllegalArgumentException("不可用积压比例必须大于降级积压比例");
            }
            // build 后生成不可变配置对象，后续所有运行线程只读它。
            return new EngineConfig<>(this);
        }
    }

    private static final class DefaultThreadFactory implements ThreadFactory {
        private final AtomicInteger idx = new AtomicInteger(1); // 默认线程编号。

        @Override
        public Thread newThread(Runnable r) {
            // 默认处理线程使用非 daemon，避免主线程提前退出导致事件处理被直接中断。
            Thread t = new Thread(r, "dis-engine-" + idx.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }

    private static final class DefaultDeadLetterHandler<E> implements DeadLetterHandler<E> {
        @Override
        public void onDeadLetter(DeadLetterEvent<E> event) {
            // 默认死信处理只输出错误，生产环境通常会替换成落库、告警或消息转储。
            System.err.println("死信 | 阶段=" + event.stageName()
                    + " | 序号=" + event.sequence()
                    + " | 尝试次数=" + event.attempts()
                    + " | 原因=" + event.cause());
            if (event.cause() != null) {
                event.cause().printStackTrace(System.err);
            }
        }
    }
}
