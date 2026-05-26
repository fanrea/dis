package com.dis.core;

import java.util.concurrent.TimeUnit;

// 生产者序列协调器。
// 核心职责：
// 1. 给生产者分配可写 sequence。
// 2. 在生产者写完槽位后发布 sequence 可见性。
// 3. 在多生产者乱序发布时，计算连续可消费上界。
public interface Sequencer {
    // 当前申请到的最大 sequence（不等于可消费上界）。
    Sequence cursorSequence();

    // 在 [lowerBound, availableSequence] 内返回连续可消费的最大 sequence。
    long getHighestPublishedSequence(long lowerBound, long availableSequence);

    // 注册 gating sequence，防止生产者覆盖未消费槽位。
    void addGatingSequence(Sequence... sequencesToAdd);

    // 阻塞申请下一个 sequence。
    long next();

    // 限时申请 sequence，超时返回负值。
    long tryNext(long timeout, TimeUnit unit);

    // 将 sequence 标记为已发布并对消费者可见。
    void publish(long sequence);
}
