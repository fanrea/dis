package com.dis.api;

@FunctionalInterface
public interface DeadLetterHandler<E> {
    void onDeadLetter(DeadLetterEvent<E> event);
}
