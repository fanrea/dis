package com.dis.strategy;

import com.dis.core.Sequence;

import java.util.Objects;

// 基于时间预算的分阶段等待策略。
// 核心逻辑：
// 1. 先在自旋阶段消耗一段纳秒预算，优先追求短等待下的低延迟。
// 2. 自旋预算耗尽后进入让步阶段，降低持续抢占 CPU 的压力。
// 3. 两段预算都耗尽后委托 fallback 策略，不在本类重复实现完整 park / blocking 逻辑。
// 4. 进度回升时记录跨调用状态，下一次 waitFor 可临时切回更激进的回弹预算。
public final class PhasedBackoffWaitStrategy implements WaitStrategy {
    private final long spinBudgetNanos; // 常规自旋阶段的时间预算。
    private final long yieldBudgetNanos; // 常规让步阶段的时间预算。
    private final long reboundSpinBudgetNanos; // 触发回弹后下一次调用使用的自旋预算。
    private final long reboundYieldBudgetNanos; // 触发回弹后下一次调用使用的让步预算。
    private final long reboundMinProgressDelta; // 跨调用累计推进到该序号差值后触发回弹。
    private final int reboundMinProgressEvents; // 跨调用观测到足够多次推进后触发回弹。
    private final WaitStrategy fallbackStrategy; // 预算耗尽后的兜底等待策略。

    private final Object reboundLock = new Object(); // 保护跨调用回弹状态。
    private long lastObservedSequence = Long.MIN_VALUE; // 上一次观测到的依赖序号。
    private long progressDeltaAcrossCalls; // 跨调用累计推进量。
    private int progressEventsAcrossCalls; // 跨调用推进事件次数。
    private volatile boolean reboundNextCall; // 下一次 waitFor 是否使用回弹预算。

    public PhasedBackoffWaitStrategy() {
        this(100_000L, 100_000L, new ParkWaitStrategy(1_000L), 32_000L, 16_000L, 4L, 2);
    }

    // 向后兼容构造器。前两个参数现在按纳秒时间预算解释，parkNanos 由 fallback 策略负责实现。
    public PhasedBackoffWaitStrategy(int spinTries, int yieldTries, long parkNanos) {
        this(spinTries, yieldTries, new ParkWaitStrategy(parkNanos), spinTries, yieldTries, 2L, 2);
    }

    // 向后兼容构造器。park 行为下沉到 fallback 策略，本类不再直接使用 maxParkNanos 和 parkGrowthFactor。
    public PhasedBackoffWaitStrategy(int spinTries,
                                     int yieldTries,
                                     long minParkNanos,
                                     long maxParkNanos,
                                     int parkGrowthFactor,
                                     int reboundSpinTries,
                                     int reboundYieldTries) {
        this(spinTries,
                yieldTries,
                minParkNanos,
                maxParkNanos,
                parkGrowthFactor,
                reboundSpinTries,
                reboundYieldTries,
                2L,
                2);
    }

    // 向后兼容构造器。maxParkNanos 和 parkGrowthFactor 仍做参数校验，但实际 park 行为由 fallback 策略拥有。
    public PhasedBackoffWaitStrategy(int spinTries,
                                     int yieldTries,
                                     long minParkNanos,
                                     long maxParkNanos,
                                     int parkGrowthFactor,
                                     int reboundSpinTries,
                                     int reboundYieldTries,
                                     long reboundMinProgressDelta,
                                     int reboundMinProgressEvents) {
        this(validateBudget(spinTries, "spinTries"),
                validateBudget(yieldTries, "yieldTries"),
                validateParkFallback(minParkNanos, maxParkNanos, parkGrowthFactor),
                validateBudget(reboundSpinTries, "reboundSpinTries"),
                validateBudget(reboundYieldTries, "reboundYieldTries"),
                reboundMinProgressDelta,
                reboundMinProgressEvents);
    }

    public PhasedBackoffWaitStrategy(long spinBudgetNanos,
                                     long yieldBudgetNanos,
                                     WaitStrategy fallbackStrategy) {
        this(spinBudgetNanos, yieldBudgetNanos, fallbackStrategy, spinBudgetNanos, yieldBudgetNanos, 2L, 2);
    }

    public PhasedBackoffWaitStrategy(long spinBudgetNanos,
                                     long yieldBudgetNanos,
                                     WaitStrategy fallbackStrategy,
                                     long reboundSpinBudgetNanos,
                                     long reboundYieldBudgetNanos,
                                     long reboundMinProgressDelta,
                                     int reboundMinProgressEvents) {
        if (spinBudgetNanos < 0L) {
            throw new IllegalArgumentException("spinBudgetNanos 必须大于等于 0");
        }
        if (yieldBudgetNanos < 0L) {
            throw new IllegalArgumentException("yieldBudgetNanos 必须大于等于 0");
        }
        if (reboundSpinBudgetNanos < 0L) {
            throw new IllegalArgumentException("reboundSpinBudgetNanos 必须大于等于 0");
        }
        if (reboundYieldBudgetNanos < 0L) {
            throw new IllegalArgumentException("reboundYieldBudgetNanos 必须大于等于 0");
        }
        if (reboundMinProgressDelta < 1L) {
            throw new IllegalArgumentException("reboundMinProgressDelta 必须大于等于 1");
        }
        if (reboundMinProgressEvents < 1) {
            throw new IllegalArgumentException("reboundMinProgressEvents 必须大于等于 1");
        }

        this.spinBudgetNanos = spinBudgetNanos;
        this.yieldBudgetNanos = yieldBudgetNanos;
        this.fallbackStrategy = Objects.requireNonNull(fallbackStrategy, "fallbackStrategy 不能为空");
        this.reboundSpinBudgetNanos = reboundSpinBudgetNanos;
        this.reboundYieldBudgetNanos = reboundYieldBudgetNanos;
        this.reboundMinProgressDelta = reboundMinProgressDelta;
        this.reboundMinProgressEvents = reboundMinProgressEvents;
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException {
        boolean rebound = consumeRebound();
        long spinBudget = rebound ? reboundSpinBudgetNanos : spinBudgetNanos;
        long yieldBudget = rebound ? reboundYieldBudgetNanos : yieldBudgetNanos;
        long availableSequence;

        availableSequence = spinUntil(sequence, dependentSequence, spinBudget);
        if (availableSequence >= sequence) {
            observeProgress(availableSequence);
            return availableSequence;
        }

        availableSequence = yieldUntil(sequence, dependentSequence, yieldBudget);
        if (availableSequence >= sequence) {
            observeProgress(availableSequence);
            return availableSequence;
        }

        availableSequence = fallbackStrategy.waitFor(sequence, cursor, dependentSequence);
        observeProgress(availableSequence);
        return availableSequence;
    }

    private boolean consumeRebound() {
        if (!reboundNextCall) {
            return false;
        }
        synchronized (reboundLock) {
            if (reboundNextCall) {
                // 回弹只影响下一次调用，避免负载恢复后长期保持激进等待。
                reboundNextCall = false;
                return true;
            }
        }
        return false;
    }

    private long spinUntil(long sequence, Sequence dependentSequence, long budgetNanos) throws InterruptedException {
        long deadline = deadlineAfter(budgetNanos);
        long observedAtStart = dependentSequence.getVolatile();
        long availableSequence;
        while ((availableSequence = dependentSequence.getVolatile()) < sequence && hasBudget(deadline)) {
            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }
            Thread.onSpinWait();
        }
        observeProgress(Math.max(observedAtStart, availableSequence));
        return availableSequence;
    }

    private long yieldUntil(long sequence, Sequence dependentSequence, long budgetNanos) throws InterruptedException {
        long deadline = deadlineAfter(budgetNanos);
        long observedAtStart = dependentSequence.getVolatile();
        long availableSequence;
        while ((availableSequence = dependentSequence.getVolatile()) < sequence && hasBudget(deadline)) {
            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }
            Thread.yield();
        }
        observeProgress(Math.max(observedAtStart, availableSequence));
        return availableSequence;
    }

    private void observeProgress(long observedSequence) {
        synchronized (reboundLock) {
            if (lastObservedSequence == Long.MIN_VALUE) {
                lastObservedSequence = observedSequence;
                return;
            }
            if (observedSequence <= lastObservedSequence) {
                return;
            }

            progressDeltaAcrossCalls = saturatingAdd(progressDeltaAcrossCalls, observedSequence - lastObservedSequence);
            progressEventsAcrossCalls++;
            lastObservedSequence = observedSequence;
            if (progressDeltaAcrossCalls >= reboundMinProgressDelta
                    || progressEventsAcrossCalls >= reboundMinProgressEvents) {
                // 满足任一滞回条件即可触发下一次调用回弹。
                reboundNextCall = true;
                progressDeltaAcrossCalls = 0L;
                progressEventsAcrossCalls = 0;
            }
        }
    }

    private static long deadlineAfter(long budgetNanos) {
        if (budgetNanos <= 0L) {
            return Long.MIN_VALUE;
        }
        long now = System.nanoTime();
        long deadline = now + budgetNanos;
        // 纳秒预算极大时可能溢出，溢出后按“近似无限预算”处理。
        return deadline < now ? Long.MAX_VALUE : deadline;
    }

    private static boolean hasBudget(long deadline) {
        return deadline != Long.MIN_VALUE && System.nanoTime() < deadline;
    }

    //防止a+b溢出
    private static long saturatingAdd(long a, long b) {
        if (Long.MAX_VALUE - a < b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    private static long validateBudget(int value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " 必须大于等于 0");
        }
        return value;
    }

    private static WaitStrategy validateParkFallback(long minParkNanos, long maxParkNanos, int parkGrowthFactor) {
        if (minParkNanos < 0L) {
            throw new IllegalArgumentException("minParkNanos 必须大于等于 0");
        }
        if (maxParkNanos < minParkNanos) {
            throw new IllegalArgumentException("maxParkNanos 必须大于等于 minParkNanos");
        }
        if (parkGrowthFactor < 1) {
            throw new IllegalArgumentException("parkGrowthFactor 必须大于等于 1");
        }
        // minParkNanos 为 0 时不能让 fallback 永远退化为自旋；有最大 park 配置时用 1ns 作为最小起点。
        long fallbackParkNanos = minParkNanos == 0L && maxParkNanos > 0L ? 1L : minParkNanos;
        return new ParkWaitStrategy(fallbackParkNanos);
    }

    @Override
    public void signalAllWhenBlocking() {
        fallbackStrategy.signalAllWhenBlocking();
    }
}
