package com.dis.runtime;

import com.dis.api.DeadLetterEvent;
import com.dis.api.DeadLetterHandler;
import com.dis.api.EventResetter;
import com.dis.api.RetryPolicy;
import com.dis.handler.ExceptionHandler;
import com.dis.handler.FatalExceptionHandler;
import com.dis.strategy.BlockingWaitStrategy;
import com.dis.strategy.WaitStrategy;

import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * 引擎不可变配置。
 *
 * 设计说明：
 * 1. 运行时会被多个线程并发读取，使用不可变对象更安全。
 * 2. 通过 Builder 统一参数校验与默认值填充。
 */
public final class EngineConfig<E> {
    private final int bufferSize;
    private final Supplier<E> eventFactory;
    private final WaitStrategy waitStrategy;
    private final ExceptionHandler<E> exceptionHandler;
    private final ThreadFactory threadFactory;
    private final EventResetter<E> eventResetter;
    private final RetryPolicy retryPolicy;
    private final DeadLetterHandler<E> deadLetterHandler;

    // 可观测性配置。
    private final boolean observabilityLogEnabled;
    private final long observabilityLogIntervalSeconds;
    private final double degradedBacklogRatio;
    private final double downBacklogRatio;

    private EngineConfig(Builder<E> builder) {
        this.bufferSize = builder.bufferSize;
        this.eventFactory = Objects.requireNonNull(builder.eventFactory, "eventFactory");
        this.waitStrategy = Objects.requireNonNullElseGet(builder.waitStrategy, BlockingWaitStrategy::new);
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
        private int bufferSize = 1024;
        private final Supplier<E> eventFactory;
        private WaitStrategy waitStrategy;
        private ExceptionHandler<E> exceptionHandler;
        private ThreadFactory threadFactory;
        private EventResetter<E> eventResetter;
        private RetryPolicy retryPolicy;
        private DeadLetterHandler<E> deadLetterHandler;

        private boolean observabilityLogEnabled = true;
        private long observabilityLogIntervalSeconds = 15;
        private double degradedBacklogRatio = 0.70;
        private double downBacklogRatio = 0.95;

        private Builder(Supplier<E> eventFactory) {
            this.eventFactory = Objects.requireNonNull(eventFactory, "eventFactory");
        }

        public Builder<E> bufferSize(int bufferSize) {
            if (Integer.bitCount(bufferSize) != 1) {
                throw new IllegalArgumentException("bufferSize must be a power of 2");
            }
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder<E> waitStrategy(WaitStrategy waitStrategy) {
            this.waitStrategy = waitStrategy;
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
                throw new IllegalArgumentException("observabilityLogIntervalSeconds must be > 0");
            }
            this.observabilityLogIntervalSeconds = observabilityLogIntervalSeconds;
            return this;
        }

        public Builder<E> degradedBacklogRatio(double degradedBacklogRatio) {
            if (degradedBacklogRatio <= 0 || degradedBacklogRatio >= 1) {
                throw new IllegalArgumentException("degradedBacklogRatio must be in (0,1)");
            }
            this.degradedBacklogRatio = degradedBacklogRatio;
            return this;
        }

        public Builder<E> downBacklogRatio(double downBacklogRatio) {
            if (downBacklogRatio <= 0 || downBacklogRatio > 1) {
                throw new IllegalArgumentException("downBacklogRatio must be in (0,1]");
            }
            this.downBacklogRatio = downBacklogRatio;
            return this;
        }

        public EngineConfig<E> build() {
            if (downBacklogRatio <= degradedBacklogRatio) {
                throw new IllegalArgumentException("downBacklogRatio must be greater than degradedBacklogRatio");
            }
            return new EngineConfig<>(this);
        }
    }

    private static final class DefaultThreadFactory implements ThreadFactory {
        private final AtomicInteger idx = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "dis-engine-" + idx.getAndIncrement());
            t.setDaemon(false);
            return t;
        }
    }

    private static final class DefaultDeadLetterHandler<E> implements DeadLetterHandler<E> {
        @Override
        public void onDeadLetter(DeadLetterEvent<E> event) {
            System.err.println("DeadLetter | stage=" + event.stageName()
                    + " | seq=" + event.sequence()
                    + " | attempts=" + event.attempts()
                    + " | cause=" + event.cause());
            if (event.cause() != null) {
                event.cause().printStackTrace(System.err);
            }
        }
    }
}
