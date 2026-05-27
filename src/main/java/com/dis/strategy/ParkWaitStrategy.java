package com.dis.strategy;

import com.dis.core.Sequence;

// 定时 park 等待策略。
// 特点：
// 1. 依赖序列未到位时使用 LockSupport.parkNanos 降低 CPU 占用。
// 2. signalAllWhenBlocking 会 unpark 所有已注册等待线程，避免只能等待 park 超时。
// 3. park 前会二次检查依赖序列，降低 signal 和 park 交错时的丢唤醒风险。
public final class ParkWaitStrategy implements WaitStrategy {
    private static final long DEFAULT_PARK_NANOS = 1_000L; // 默认每次最多 park 1 微秒。

    private final long parkNanos; // 每次 park 的最大时长。
    private final Parker parker = new Parker(); // 统一管理等待线程注册和唤醒。

    public ParkWaitStrategy() {
        this(DEFAULT_PARK_NANOS);
    }

    public ParkWaitStrategy(long parkNanos) {
        if (parkNanos < 0L) {
            throw new IllegalArgumentException("parkNanos 必须大于等于 0");
        }
        this.parkNanos = parkNanos;
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException {
        long availableSequence;
        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            if (Thread.interrupted()) {
                throw new InterruptedException("等待序号时线程被中断");
            }

            if (parkNanos > 0L) {
                parker.parkNanos(parkNanos, () -> dependentSequence.getVolatile() < sequence);
            } else {
                Thread.onSpinWait();
            }
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        parker.signalAll();
    }
}
