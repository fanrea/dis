package com.dis.api;

@FunctionalInterface
public interface BusinessEventHandler<E> {
    void onEvent(E event, long sequence) throws Exception;
}
