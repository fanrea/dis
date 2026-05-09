package com.dis.api;

@FunctionalInterface
public interface EventResetter<E> {
    void reset(E event);
}
