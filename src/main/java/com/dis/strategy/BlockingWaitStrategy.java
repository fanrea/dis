package com.dis.strategy;

import com.dis.core.Sequence;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

// 阻塞等待策略。
// 特点：
// 1. 游标未到位时使用 Condition 阻塞，CPU 占用低。
// 2. 依赖序列阶段使用轻量自旋，避免频繁锁切换。
public class BlockingWaitStrategy implements WaitStrategy {
    private static final long DEFAULT_AWAIT_NANOS = TimeUnit.MICROSECONDS.toNanos(50);

    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();
    private final long awaitNanos;

    public BlockingWaitStrategy() {
        this(DEFAULT_AWAIT_NANOS);
    }

    public BlockingWaitStrategy(long awaitNanos) {
        if (awaitNanos <= 0L) {
            throw new IllegalArgumentException("awaitNanos 必须大于 0");
        }
        this.awaitNanos = awaitNanos;
    }

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException {
        if (cursor.getVolatile() < sequence) {
            lock.lock();
            try {
                while (cursor.getVolatile() < sequence) {
                    processorNotifyCondition.awaitNanos(awaitNanos);
                    if (Thread.interrupted()) {
                        throw new InterruptedException("等待 cursor 时被中断");
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        long availableSequence;
        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            if (Thread.interrupted()) {
                throw new InterruptedException("等待依赖序列时被中断");
            }
            Thread.onSpinWait();
        }
        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking() {
        lock.lock();
        try {
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
