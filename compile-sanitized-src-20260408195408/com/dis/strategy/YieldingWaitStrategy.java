package com.dis.strategy;

import com.dis.core.Sequence;

/**
 * 让步等待策略。
 *
 * 特点：
 * 1. 先短暂自旋，降低上下文切换。
 * 2. 再主动 yield，让出 CPU。
 * 3. 适合低延迟场景，但 CPU 占用通常更高。
 */
public class YieldingWaitStrategy implements WaitStrategy {
    private static final int SPIN_TRIES = 100;

    @Override
    public long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException {
        long availableSequence;
        int counter = SPIN_TRIES;

        while ((availableSequence = dependentSequence.getVolatile()) < sequence) {
            if (Thread.interrupted()) {
                throw new InterruptedException("interrupted while waiting");
            }
            counter = applyWaitMethod(counter);
        }
        return availableSequence;
    }

    private int applyWaitMethod(int counter) {
        if (counter == 0) {
            Thread.yield();
        } else {
            --counter;
            Thread.onSpinWait();
        }
        return counter;
    }

    @Override
    public void signalAllWhenBlocking() {
        // 非阻塞策略，无需唤醒。
    }
}
