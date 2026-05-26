package com.dis.strategy;

import com.dis.core.Sequence;

import java.util.concurrent.locks.LockSupport;

/**
 * 自适应分阶段退避等待策略。
 *
 * <p>阶段顺序：
 * 1) 自旋（spin）：有限次快速轮询，优先追求低延迟。
 * 2) 让步（yield）：让出时间片，降低持续自旋的 CPU 压力。
 * 3) 停顿（park）：可选指数退避，适合长时间空闲时节省 CPU。
 *
 * <p>核心语义：
 * 1) 空闲期会逐步退化到更省 CPU 的阶段。
 * 2) 负载恢复后可回弹到更激进阶段，减少恢复期延迟。
 * 3) 回弹带滞回阈值，避免“单次进度抖动”导致频繁震荡。
 */
public final class PhasedBackoffWaitStrategy implements WaitStrategy {
    // 进入下一阶段前的预算次数。
    private final int spinTries;
    private final int yieldTries;

    // park 退避参数。
    private final long minParkNanos;
    private final long maxParkNanos;
    private final int parkGrowthFactor;

    // 回弹触发后重新分配的 spin/yield 预算。
    private final int reboundSpinTries;
    private final int reboundYieldTries;

    // park 阶段的回弹滞回阈值：
    // - 序号增量阈值：累计推进到该值后可回弹
    // - 推进事件阈值：观测到足够多次推进后可回弹
    // 满足任一阈值即可回弹。
    private final long reboundMinProgressDelta;
    private final int reboundMinProgressEvents;

    public PhasedBackoffWaitStrategy() {
        this(100, 100, 1_000L, 50_000L, 2, 32, 16, 4L, 2);
    }

    /**
     * 向后兼容构造器：固定 park 时长，不做指数增长。
     */
    public PhasedBackoffWaitStrategy(int spinTries, int yieldTries, long parkNanos) {
        this(spinTries, yieldTries, parkNanos, parkNanos, 1, spinTries, yieldTries, 2L, 2);
    }

    /**
     * 向后兼容构造器：保留历史参数形态，默认滞回阈值。
     */
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

    public PhasedBackoffWaitStrategy(int spinTries,
                                     int yieldTries,
                                     long minParkNanos,
                                     long maxParkNanos,
                                     int parkGrowthFactor,
                                     int reboundSpinTries,
                                     int reboundYieldTries,
                                     long reboundMinProgressDelta,
                                     int reboundMinProgressEvents) {
        if (spinTries < 0) {
            throw new IllegalArgumentException("spinTries 必须大于等于 0");
        }
        if (yieldTries < 0) {
            throw new IllegalArgumentException("yieldTries 必须大于等于 0");
        }
        if (minParkNanos < 0L) {
            throw new IllegalArgumentException("minParkNanos 必须大于等于 0");
        }
        if (maxParkNanos < minParkNanos) {
            throw new IllegalArgumentException("maxParkNanos 必须大于等于 minParkNanos");
        }
        if (parkGrowthFactor < 1) {
            throw new IllegalArgumentException("parkGrowthFactor 必须大于等于 1");
        }
        if (reboundSpinTries < 0) {
            throw new IllegalArgumentException("reboundSpinTries 必须大于等于 0");
        }
        if (reboundYieldTries < 0) {
            throw new IllegalArgumentException("reboundYieldTries 必须大于等于 0");
        }
        if (reboundMinProgressDelta < 1L) {
            throw new IllegalArgumentException("reboundMinProgressDelta 必须大于等于 1");
        }
        if (reboundMinProgressEvents < 1) {
            throw new IllegalArgumentException("reboundMinProgressEvents 必须大于等于 1");
        }

        this.spinTries = spinTries;
        this.yieldTries = yieldTries;
        this.minParkNanos = minParkNanos;
        this.maxParkNanos = maxParkNanos;
        this.parkGrowthFactor = parkGrowthFactor;
        this.reboundSpinTries = reboundSpinTries;
        this.reboundYieldTries = reboundYieldTries;
        this.reboundMinProgressDelta = reboundMinProgressDelta;
        this.reboundMinProgressEvents = reboundMinProgressEvents;
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException {
        long availableSequence;

        int spinCounter = spinTries;
        int yieldCounter = yieldTries;
        long parkBudgetNanos = minParkNanos;

        long lastObserved = dependentSequence.getVolatile();
        long progressDeltaWhileParked = 0L;
        int progressEventsWhileParked = 0;

        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }

            // 优先观测进度推进，再决定是否回弹。
            if (availableSequence > lastObserved) {
                long delta = availableSequence - lastObserved;
                lastObserved = availableSequence;

                if (isParkStage(spinCounter, yieldCounter)) {
                    progressDeltaWhileParked = saturatingAdd(progressDeltaWhileParked, delta);
                    progressEventsWhileParked++;

                    // 达到滞回阈值才回弹，避免一次推进就激进回弹。
                    if (shouldRebound(progressDeltaWhileParked, progressEventsWhileParked)) {
                        spinCounter = reboundSpinTries;
                        yieldCounter = reboundYieldTries;
                        parkBudgetNanos = minParkNanos;
                        progressDeltaWhileParked = 0L;
                        progressEventsWhileParked = 0;
                        Thread.onSpinWait();
                        continue;
                    }
                }
            } else if (isParkStage(spinCounter, yieldCounter)) {
                // 需要连续新推进才能累计“推进事件次数”阈值。
                progressEventsWhileParked = 0;
            }

            if (spinCounter > 0) {
                spinCounter--;
                progressDeltaWhileParked = 0L;
                progressEventsWhileParked = 0;
                Thread.onSpinWait();
                continue;
            }

            if (yieldCounter > 0) {
                yieldCounter--;
                progressDeltaWhileParked = 0L;
                progressEventsWhileParked = 0;
                Thread.yield();
                continue;
            }

            if (parkBudgetNanos > 0L) {
                LockSupport.parkNanos(parkBudgetNanos);
                if (parkGrowthFactor > 1 && parkBudgetNanos < maxParkNanos) {
                    long grown;
                    try {
                        grown = Math.multiplyExact(parkBudgetNanos, (long) parkGrowthFactor);
                    } catch (ArithmeticException overflow) {
                        grown = Long.MAX_VALUE;
                    }
                    parkBudgetNanos = Math.min(maxParkNanos, grown);
                }
            } else {
                Thread.onSpinWait();
            }
        }

        return availableSequence;
    }

    private boolean isParkStage(int spinCounter, int yieldCounter) {
        return spinCounter <= 0 && yieldCounter <= 0;
    }

    private boolean shouldRebound(long progressDeltaWhileParked, int progressEventsWhileParked) {
        return progressDeltaWhileParked >= reboundMinProgressDelta
                || progressEventsWhileParked >= reboundMinProgressEvents;
    }

    private static long saturatingAdd(long a, long b) {
        if (Long.MAX_VALUE - a < b) {
            return Long.MAX_VALUE;
        }
        return a + b;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 本策略不依赖阻塞原语，无需显式唤醒。
    }
}
