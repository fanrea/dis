package com.dis.api;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

// 事件发布接口。
public interface EventPublisher<E> {
    // 阻塞发布：直到拿到可用 sequence 并完成发布。
    void publishEvent(EventTranslator<E> translator);

    // 限时发布：超时时返回 false，不调用 translator。
    default boolean tryPublishEvent(EventTranslator<E> translator, long timeout, TimeUnit unit) {
        Objects.requireNonNull(translator, "translator");
        Objects.requireNonNull(unit, "unit");
        throw new UnsupportedOperationException("不支持限时发布");
    }
}
