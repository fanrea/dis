package com.dis.strategy;

import com.dis.core.Sequence;

import java.util.Objects;

// 基于时间预算的分阶段等待策略。
// 核心逻辑：
// 1. 等待路径保持 spin -> yield -> fallbackStrategy 三阶段。
// 2. spin/yield 只消耗当前调用的时间预算，不负责完整 park 或阻塞逻辑。
// 3. 只有已经退到 fallback 后观察到进度恢复，才会让下一次 waitFor 使用更大的 active wait 回弹预算。
// 4. 回弹状态按消费线程隔离，避免多个 processor 共享同一个策略实例时互相污染。
public final class PhasedBackoffWaitStrategy implements WaitStrategy {
    private static final int SPIN_CHECK_MASK = 63; // spin 热循环每 64 次检查一次时间和中断。

    private final long spinBudgetNanos; // 常规自旋阶段的时间预算。
    private final long yieldBudgetNanos; // 常规让步阶段的时间预算。
    private final long reboundSpinBudgetNanos; // 触发回弹后下一次调用使用的自旋预算。
    private final long reboundYieldBudgetNanos; // 触发回弹后下一次调用使用的让步预算。
    private final long reboundMinProgressDelta; // fallback 后累计推进到该序号差值时触发回弹。
    private final int reboundMinProgressEvents; // fallback 后观测到足够多次推进时触发回弹。
    private final WaitStrategy fallbackStrategy; // 预算耗尽后的兜底等待策略。
    private final ThreadLocal<ReboundState> reboundState = ThreadLocal.withInitial(ReboundState::new);

    public PhasedBackoffWaitStrategy() {
        this(32_000L, 16_000L, new ParkWaitStrategy(1_000L), 100_000L, 100_000L, 4L, 2);
    }

    // 已废弃兼容构造器。spinTries/yieldTries 现在按纳秒预算解释，不再表示循环次数。
    @Deprecated
    public PhasedBackoffWaitStrategy(int spinTries, int yieldTries, long parkNanos) {
        this(spinTries, yieldTries, new ParkWaitStrategy(parkNanos), spinTries, yieldTries, 2L, 2);
    }

    // 已废弃兼容构造器。park 行为下沉到 fallback 策略，本类不再直接使用 maxParkNanos 和 parkGrowthFactor。
    @Deprecated
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

    // 已废弃兼容构造器。maxParkNanos 和 parkGrowthFactor 仍做参数校验，但实际 park 行为由 fallback 策略拥有。
    @Deprecated
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
        if (reboundSpinBudgetNanos < spinBudgetNanos) {
            throw new IllegalArgumentException("reboundSpinBudgetNanos 必须大于等于 spinBudgetNanos");
        }
        if (reboundYieldBudgetNanos < yieldBudgetNanos) {
            throw new IllegalArgumentException("reboundYieldBudgetNanos 必须大于等于 yieldBudgetNanos");
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
        ReboundState state = reboundState.get();
        boolean rebound = consumeRebound(state);
        long spinBudget = rebound ? reboundSpinBudgetNanos : spinBudgetNanos;
        long yieldBudget = rebound ? reboundYieldBudgetNanos : yieldBudgetNanos;

        long availableSequence = spinUntil(sequence, dependentSequence, spinBudget);
        observeProgress(state, availableSequence, WaitPhase.SPIN);
        if (availableSequence >= sequence) {
            return availableSequence;
        }

        availableSequence = yieldUntil(sequence, dependentSequence, yieldBudget);
        observeProgress(state, availableSequence, WaitPhase.YIELD);
        if (availableSequence >= sequence) {
            return availableSequence;
        }

        availableSequence = fallbackStrategy.waitFor(sequence, cursor, dependentSequence);
        observeProgress(state, availableSequence, WaitPhase.FALLBACK);
        return availableSequence;
    }

    private boolean consumeRebound(ReboundState state) {
        if (!state.reboundNextCall) {
            return false;
        }
        state.reboundNextCall = false;
        return true;
    }

    private long spinUntil(long sequence, Sequence dependentSequence, long budgetNanos) throws InterruptedException {
        if (budgetNanos == 0L) {
            return dependentSequence.getVolatile();
        }

        long deadline = deadlineAfter(budgetNanos);
        long availableSequence;
        int counter = 0;
        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            // spin 是热路径，时间和中断都降频检查，避免每轮 nanoTime 和中断状态读。
            if ((++counter & SPIN_CHECK_MASK) == 0) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("等待序号时线程被中断");
                }
                if (!hasBudget(deadline)) {
                    break;
                }
            }
            Thread.onSpinWait();
        }
        return availableSequence;
    }

    private long yieldUntil(long sequence, Sequence dependentSequence, long budgetNanos) throws InterruptedException {
        if (budgetNanos == 0L) {
            return dependentSequence.getVolatile();
        }

        long deadline = deadlineAfter(budgetNanos);
        long availableSequence;
        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            // yield 本身已经较重，这里每轮检查时间和中断，保证预算边界和中断响应更明确。
            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }
            if (!hasBudget(deadline)) {
                break;
            }
            Thread.yield();
        }
        return availableSequence;
    }

    private void observeProgress(ReboundState state, long observedSequence, WaitPhase phase) {
        if (state.lastObservedSequence == Long.MIN_VALUE) {
            state.lastObservedSequence = observedSequence;
            return;
        }
        if (observedSequence <= state.lastObservedSequence) {
            return;
        }

        long delta = observedSequence - state.lastObservedSequence;
        state.lastObservedSequence = observedSequence;
        if (phase != WaitPhase.FALLBACK) {
            return;
        }

        state.progressDeltaAcrossCalls = saturatingAdd(state.progressDeltaAcrossCalls, delta);
        state.progressEventsAcrossCalls++;
        if (state.progressDeltaAcrossCalls >= reboundMinProgressDelta
                || state.progressEventsAcrossCalls >= reboundMinProgressEvents) {
            state.reboundNextCall = true;
            state.progressDeltaAcrossCalls = 0L;
            state.progressEventsAcrossCalls = 0;
        }
    }

    private static long deadlineAfter(long budgetNanos) {
        long now = System.nanoTime();
        long deadline = now + budgetNanos;
        // 纳秒预算极大时可能溢出，溢出后按“近似无限预算”处理。
        return deadline < now ? Long.MAX_VALUE : deadline;
    }

    private static boolean hasBudget(long deadline) {
        return System.nanoTime() < deadline;
    }

    // 防止 a + b 溢出。
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

    private enum WaitPhase {
        SPIN,
        YIELD,
        FALLBACK
    }

    private static final class ReboundState {
        private long lastObservedSequence = Long.MIN_VALUE;
        private long progressDeltaAcrossCalls;
        private int progressEventsAcrossCalls;
        private boolean reboundNextCall;
    }
}
