package com.dis.api;

import java.util.concurrent.TimeUnit;

public interface EventEngine<E> extends AutoCloseable {
    EventChain<E> handleEventsWith(BusinessEventHandler<E>... handlers);

    EventPublisher<E> publisher();

    void start();

    void shutdown();

    boolean awaitShutdown(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 获取当前指标快照。
     */
    EngineMetricsSnapshot metricsSnapshot();

    /**
     * 获取当前健康报告。
     */
    EngineHealthReport healthReport();

    @Override
    default void close() {
        shutdown();
    }
}
