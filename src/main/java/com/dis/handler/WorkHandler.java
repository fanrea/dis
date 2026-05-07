package com.dis.handler;

public interface WorkHandler<T> {
    void onEvent(T event) throws Exception;
}