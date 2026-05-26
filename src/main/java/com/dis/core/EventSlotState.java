package com.dis.core;

// EventSlot 状态机。
// 状态流转：
// EMPTY -> TRANSLATING -> READY
// EMPTY -> TRANSLATING -> TRANSLATE_FAILED
// READY 和 TRANSLATE_FAILED 都是可发布终态；消费者必须处理或跳过它们，保证 sequence 可以继续推进。
public enum EventSlotState {
    EMPTY, // 初始态，表示槽位还没有进入本轮发布流程。
    TRANSLATING, // 生产者已经占用槽位，正在把业务数据写入 event。
    READY, // 业务数据写入成功，消费者可以正常执行业务 handler。
    TRANSLATE_FAILED // 业务数据写入失败，消费者需要跳过该 sequence 并记录观测指标。
}
