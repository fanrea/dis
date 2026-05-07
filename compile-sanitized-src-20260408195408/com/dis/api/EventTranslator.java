package com.dis.api;

@FunctionalInterface
public interface EventTranslator<E> {
    void translateTo(E event, long sequence);
}
