package com.dis.handler;

public interface EventHandler<E> {
    void onEvent(E event, long sequence) throws Exception;
}