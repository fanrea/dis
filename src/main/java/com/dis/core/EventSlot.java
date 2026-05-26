package com.dis.core;

// RingBuffer 槽位包装。
// 核心逻辑：
// 1. 业务对象本身会复用，EventSlot 额外记录本轮发布状态。
// 2. translator 成功时进入 READY，失败时进入 TRANSLATE_FAILED，消费者据此处理或跳过。
// 3. sequence、时间戳和异常用于诊断槽位复用、发布失败和处理延迟。
public final class EventSlot<E> {
    private final E event; // 真正的业务事件对象，构造后一直复用。
    private volatile long sequence = -1L; // 被发布到该槽位的逻辑序号，便于诊断 slot 复用问题。
    private volatile EventSlotState state = EventSlotState.EMPTY; // 当前槽位状态，消费者根据它决定处理还是跳过。
    private volatile Throwable publishError; // translator 失败时记录原始异常，供消费者观测并跳过。
    private volatile long publishNanos; // 开始翻译时间。
    private volatile long visibleNanos; // 状态对消费者可见时间。

    public EventSlot(E event) {
        // event 通常由 EngineConfig.eventFactory 预创建，后续发布只重置字段。
        this.event = event;
    }

    public E event() {
        return event;
    }

    public long sequence() {
        return sequence;
    }

    public EventSlotState state() {
        return state;
    }

    public Throwable publishError() {
        return publishError;
    }

    public long publishNanos() {
        return publishNanos;
    }

    public long visibleNanos() {
        return visibleNanos;
    }

    public void resetForTranslate(long sequence) {
        // 每次 claim 到新 sequence 后先清理上一轮残留，再进入 TRANSLATING。
        // 注意：此时 sequence 还没有 publish，对消费者不可见；真正可见由 Sequencer.publish 建立。

        this.sequence = sequence;
        this.publishError = null;
        this.publishNanos = System.nanoTime();
        this.visibleNanos = 0L;
        this.state = EventSlotState.TRANSLATING;
    }

    public void markReady() {
        // 仅在 translator 成功后进入 READY，消费者才会执行业务 handler。
        // 状态写入发生在 publish 之前，随后 publish 的 release-store 会把该状态安全发布给消费者。

        this.visibleNanos = System.nanoTime();
        this.state = EventSlotState.READY;
    }

    public void markTranslateFailed(Throwable publishError) {
        // translator 失败也要落状态，让下游能够“有序跳过”而不是卡住序列推进。
        // 失败槽位仍会被 publish；消费者看到 TRANSLATE_FAILED 后记录观测事件并推进自己的 sequence。

        this.publishError = publishError;
        this.visibleNanos = System.nanoTime();
        this.state = EventSlotState.TRANSLATE_FAILED;
    }
}
