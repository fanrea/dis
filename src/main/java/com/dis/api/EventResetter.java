package com.dis.api;

// 事件复用前的重置器。
// RingBuffer 槽位复用时，先 reset 再 translate，避免旧字段污染新事件。
@FunctionalInterface
public interface EventResetter<E> {
    void reset(E event);
}
