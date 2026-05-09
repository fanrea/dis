package com.dis.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

public interface EventPublisher<E> {
    void publishEvent(EventTranslator<E> translator);

    default boolean tryPublishEvent(EventTranslator<E> translator, long timeout, TimeUnit unit) {
        Objects.requireNonNull(translator, "translator");
        Objects.requireNonNull(unit, "unit");
        throw new UnsupportedOperationException("tryPublishEvent is not supported");
    }
}
