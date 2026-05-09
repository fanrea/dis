package com.dis.api;

import java.util.concurrent.TimeUnit;

import com.dis.handler.WorkHandler;

public interface EventEngine<E> extends AutoCloseable {
    default EventChain<E> handleEventsWith(BusinessEventHandler<E>... handlers) {
        return handleEventsWith(null, handlers);
    }

    EventChain<E> handleEventsWith(String stageName, BusinessEventHandler<E>... handlers);

    default EventChain<E> handleEventsWithWorkerPool(int workerCount, WorkHandler<E> handler) {
        return handleEventsWithWorkerPool(null, workerCount, handler);
    }

    EventChain<E> handleEventsWithWorkerPool(String stageName, int workerCount, WorkHandler<E> handler);

    EventPublisher<E> publisher();

    void start();

    void shutdown();

    void shutdownGracefully();

    boolean shutdownGracefully(long timeout, TimeUnit unit) throws InterruptedException;

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
