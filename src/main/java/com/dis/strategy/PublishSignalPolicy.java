package com.dis.strategy;

// 发布唤醒策略。
// 控制 publish 后何时调用 waitStrategy.signalAllWhenBlocking()。
// 注意：本策略只决定“是否调用 signal”，真正能否唤醒消费者取决于具体 WaitStrategy：
// 1. BlockingWaitStrategy 会 signal Condition。
// 2. ParkWaitStrategy 会 unpark 已注册线程。
// 3. YieldingWaitStrategy 只有配置 park 时才有可唤醒等待线程。
public interface PublishSignalPolicy {
    boolean shouldSignal(long sequence);
}
