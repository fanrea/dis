package com.dis.strategy;

import com.dis.core.Sequence;

import java.util.concurrent.locks.LockSupport;

/**
 * 可调参数的让步等待策略。
 *
 * <p>阶段顺序：
 * 1) 有界自旋
 * 2) 有界/无界让步
 * 3) 可选固定时长 park
 *
 * <p>相较旧实现：
 * 1) 各阶段预算可配置
 * 2) 是否退化到 park 可配置
 * 3) 深阶段回弹带滞回阈值，不再单次推进即回弹
 */
public class YieldingWaitStrategy implements WaitStrategy {
    private final int spinTries;
    private final int yieldTries;
    private final long parkNanos;

    private final int reboundSpinTries;
    private final int reboundYieldTries;
    private final long reboundMinProgressDelta;
    private final int reboundMinProgressEvents;

    public YieldingWaitStrategy() {
        this(100, Integer.MAX_VALUE, 0L, 32, 16, 2L, 2);
    }

    public YieldingWaitStrategy(int spinTries, int yieldTries, long parkNanos) {
        this(spinTries,
                yieldTries,
                parkNanos,
                spinTries,
                Math.min(yieldTries, 64),
                2L,
                2);
    }

    public YieldingWaitStrategy(int spinTries,
                                int yieldTries,
                                long parkNanos,
                                int reboundSpinTries,
                                int reboundYieldTries) {
        this(spinTries,
                yieldTries,
                parkNanos,
                reboundSpinTries,
                reboundYieldTries,
                2L,
                2);
    }

    public YieldingWaitStrategy(int spinTries,
                                int yieldTries,
                                long parkNanos,
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
        if (parkNanos < 0L) {
            throw new IllegalArgumentException("parkNanos 必须大于等于 0");
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
        this.parkNanos = parkNanos;
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

        long lastObserved = dependentSequence.getVolatile();
        long progressDeltaInDeepPhase = 0L;
        int progressEventsInDeepPhase = 0;

        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }

            if (availableSequence > lastObserved) {
                long delta = availableSequence - lastObserved;
                lastObserved = availableSequence;

                if (isDeepPhase(spinCounter, yieldCounter)) {
                    progressDeltaInDeepPhase = saturatingAdd(progressDeltaInDeepPhase, delta);
                    progressEventsInDeepPhase++;
                    if (shouldRebound(progressDeltaInDeepPhase, progressEventsInDeepPhase)) {
                        spinCounter = reboundSpinTries;
                        yieldCounter = reboundYieldTries;
                        progressDeltaInDeepPhase = 0L;
                        progressEventsInDeepPhase = 0;
                        Thread.onSpinWait();
                        continue;
                    }
                }
            } else if (isDeepPhase(spinCounter, yieldCounter)) {
                // 需要连续新推进才能累计“推进事件次数”阈值。
                progressEventsInDeepPhase = 0;
            }

            if (spinCounter > 0) {
                spinCounter--;
                progressDeltaInDeepPhase = 0L;
                progressEventsInDeepPhase = 0;
                Thread.onSpinWait();
                continue;
            }

            if (yieldCounter > 0) {
                // Integer.MAX_VALUE 表示近似“无限让步”阶段。
                if (yieldCounter != Integer.MAX_VALUE) {
                    yieldCounter--;
                }
                progressDeltaInDeepPhase = 0L;
                progressEventsInDeepPhase = 0;
                Thread.yield();
                continue;
            }

            if (parkNanos > 0L) {
                LockSupport.parkNanos(parkNanos);
            } else {
                Thread.onSpinWait();
            }
        }

        return availableSequence;
    }

    private boolean isDeepPhase(int spinCounter, int yieldCounter) {
        return spinCounter <= 0 && yieldCounter <= 0;
    }

    private boolean shouldRebound(long progressDeltaInDeepPhase, int progressEventsInDeepPhase) {
        return progressDeltaInDeepPhase >= reboundMinProgressDelta
                || progressEventsInDeepPhase >= reboundMinProgressEvents;
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
