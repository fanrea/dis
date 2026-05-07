package com.dis.api;

public interface EventPublisher<E> {
    void publishEvent(EventTranslator<E> translator);
}
