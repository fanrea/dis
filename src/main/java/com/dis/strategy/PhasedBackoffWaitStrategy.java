package com.dis.strategy;

import com.dis.core.Sequence;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * 基于时间窗口的分阶段等待策略。
 *
 * 该策略只在单次 waitFor 调用内做退避：
 * 1. 短时间 busy spin，优化短等待延迟；
 * 2. 超过 spin 窗口后 yield，降低 CPU 抢占；
 * 3. 超过 yield 窗口后委托 fallbackStrategy。
 *
 * 本类不维护跨调用自适应状态，避免多个消费者共享策略实例时发生状态串扰。
 */
public final class PhasedBackoffWaitStrategy implements WaitStrategy {
    private static final int SPIN_TRIES = 10_000; // 每轮时间检查之间允许的自旋次数。
    private static final long TRY_NANOS = 1_000L; // try 参数换算成 active wait 窗口时使用的纳秒权重。

    private final long spinTimeoutNanos; // 从开始等待到进入 yield 阶段的时间窗口。
    private final long yieldTimeoutNanos; // 从开始等待到进入 fallback 阶段的总时间窗口。
    private final WaitStrategy fallbackStrategy; // 超过 active wait 窗口后的兜底策略。

    public PhasedBackoffWaitStrategy() {
        this(100L, 100L, TimeUnit.MICROSECONDS, new ParkWaitStrategy(1_000L));
    }

    public PhasedBackoffWaitStrategy(long spinTimeout,
                                     long yieldTimeout,
                                     TimeUnit unit,
                                     WaitStrategy fallbackStrategy) {
        if (spinTimeout < 0L) {
            throw new IllegalArgumentException("spinTimeout 必须大于等于 0");
        }
        if (yieldTimeout < 0L) {
            throw new IllegalArgumentException("yieldTimeout 必须大于等于 0");
        }
        Objects.requireNonNull(unit, "unit 不能为空");
        this.fallbackStrategy = Objects.requireNonNull(fallbackStrategy, "fallbackStrategy 不能为空");

        this.spinTimeoutNanos = unit.toNanos(spinTimeout);
        long yieldBudgetNanos = unit.toNanos(yieldTimeout);
        this.yieldTimeoutNanos = saturatingAdd(this.spinTimeoutNanos, yieldBudgetNanos);
    }

    /**
     * 使用纳秒级 active wait 窗口和自定义 fallback 策略。
     */
    public PhasedBackoffWaitStrategy(long spinBudgetNanos,
                                     long yieldBudgetNanos,
                                     WaitStrategy fallbackStrategy) {
        this(validateNanos(spinBudgetNanos, "spinBudgetNanos"),
                validateNanos(yieldBudgetNanos, "yieldBudgetNanos"),
                TimeUnit.NANOSECONDS,
                fallbackStrategy);
    }

    public static PhasedBackoffWaitStrategy withPark(long spinTimeout,
                                                     long yieldTimeout,
                                                     TimeUnit unit,
                                                     long parkNanos) {
        return new PhasedBackoffWaitStrategy(spinTimeout, yieldTimeout, unit, new ParkWaitStrategy(parkNanos));
    }

    public static PhasedBackoffWaitStrategy withLock(long spinTimeout,
                                                     long yieldTimeout,
                                                     TimeUnit unit) {
        return new PhasedBackoffWaitStrategy(spinTimeout, yieldTimeout, unit, new BlockingWaitStrategy());
    }

    /**
     * 使用 try 风格参数配置 active wait 窗口。
     *
     * spinTries 和 yieldTries 会按固定纳秒权重换算为时间窗口，parkNanos 指定 fallback
     * 每次挂起的时长。
     */
    @Deprecated
    public PhasedBackoffWaitStrategy(int spinTries, int yieldTries, long parkNanos) {
        this(triesToNanos(spinTries),
                triesToNanos(yieldTries),
                TimeUnit.NANOSECONDS,
                new ParkWaitStrategy(parkNanos));
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException {
        long availableSequence = dependentSequence.getVolatile();
        if (availableSequence >= sequence) {
            return availableSequence;
        }
        if (yieldTimeoutNanos == 0L) {
            return fallbackStrategy.waitFor(sequence, cursor, dependentSequence);
        }

        long startTime = System.nanoTime();
        int counter = SPIN_TRIES;

        while (true) {
            availableSequence = dependentSequence.getVolatile();
            if (availableSequence >= sequence) {
                return availableSequence;
            }

            if (--counter == 0) {
                long elapsed = System.nanoTime() - startTime;
                if (elapsed > yieldTimeoutNanos) {
                    return fallbackStrategy.waitFor(sequence, cursor, dependentSequence);
                }
                if (elapsed > spinTimeoutNanos) {
                    Thread.yield();
                } else {
                    Thread.onSpinWait();
                }
                counter = SPIN_TRIES;
            } else {
                Thread.onSpinWait();
            }

            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }
        }
    }

    private static long triesToNanos(int tries) {
        if (tries < 0) {
            throw new IllegalArgumentException("tries 必须大于等于 0");
        }
        return Math.multiplyExact((long) tries, TRY_NANOS);
    }

    private static long validateNanos(long nanos, String name) {
        if (nanos < 0L) {
            throw new IllegalArgumentException(name + " 必须大于等于 0");
        }
        return nanos;
    }

    private static long saturatingAdd(long a, long b) {
        if (Long.MAX_VALUE - a < b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    @Override
    public void signalAllWhenBlocking() {
        fallbackStrategy.signalAllWhenBlocking();
    }
}
