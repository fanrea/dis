package com.dis.api;

import com.dis.handler.WorkHandler;

import java.util.concurrent.TimeUnit;

// 事件引擎顶层接口。
public interface EventEngine<E> extends AutoCloseable {
    default EventChain<E> handleEventsWith(BusinessEventHandler<E>... handlers) {
        return handleEventsWith(null, handlers);
    }

    // 配置广播型 stage：同一 stage 内每个 handler 都会处理每个事件。
    EventChain<E> handleEventsWith(String stageName, BusinessEventHandler<E>... handlers);

    default EventChain<E> handleEventsWithWorkerPool(int workerCount, WorkHandler<E> handler) {
        return handleEventsWithWorkerPool(null, workerCount, handler);
    }

    // 配置 worker pool stage：每个事件只会被其中一个 worker 处理。
    EventChain<E> handleEventsWithWorkerPool(String stageName, int workerCount, WorkHandler<E> handler);

    // 获取发布器。
    EventPublisher<E> publisher();

    // 启动引擎与各 stage 线程。
    void start();

    // 快速停机：不保证 drain 完成。
    void shutdown();

    // 优雅停机：拒绝新事件并等待已发布事件尽量处理完成。
    void shutdownGracefully();

    // 带超时的优雅停机。
    boolean shutdownGracefully(long timeout, TimeUnit unit) throws InterruptedException;

    // 等待处理线程退出。
    boolean awaitShutdown(long timeout, TimeUnit unit) throws InterruptedException;

    // 获取当前指标快照。
    EngineMetricsSnapshot metricsSnapshot();

    // 获取当前健康报告。
    EngineHealthReport healthReport();

    @Override
    default void close() {
        shutdown();
    }
}
