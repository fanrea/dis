package com.dis.strategy;

// 发布唤醒策略。
// 控制 publish 后何时调用 waitStrategy.signalAllWhenBlocking()。
public interface PublishSignalPolicy {
    boolean shouldSignal(long sequence);
}
