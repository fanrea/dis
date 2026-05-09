package com.dis.core;

/**
 * 环形槽位。
 */
public final class EventSlot<E> {
    private final E event;
    private volatile long sequence = -1L;
    private volatile EventSlotState state = EventSlotState.EMPTY;
    private volatile Throwable publishError;
    private volatile long publishNanos;
    private volatile long visibleNanos;

    public EventSlot(E event) {
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
        this.sequence = sequence;
        this.publishError = null;
        this.publishNanos = System.nanoTime();
        this.visibleNanos = 0L;
        this.state = EventSlotState.TRANSLATING;
    }

    public void markReady() {
        this.visibleNanos = System.nanoTime();
        this.state = EventSlotState.READY;
    }

    public void markTranslateFailed(Throwable publishError) {
        this.publishError = publishError;
        this.visibleNanos = System.nanoTime();
        this.state = EventSlotState.TRANSLATE_FAILED;
    }
}
