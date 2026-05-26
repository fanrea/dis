package com.dis.strategy;

import com.dis.core.Sequence;

// 消费者等待策略。
// waitFor 返回可用上界，由处理器再结合 sequencer 的发布可见性计算安全消费区间。
public interface WaitStrategy {
    long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException;

    void signalAllWhenBlocking();
}
