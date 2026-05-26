package com.dis.api;

// 死信处理回调。
@FunctionalInterface
public interface DeadLetterHandler<E> {
    void onDeadLetter(DeadLetterEvent<E> event);
}
