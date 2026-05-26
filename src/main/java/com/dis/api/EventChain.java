package com.dis.api;

import com.dis.handler.WorkHandler;

// Stage 链式编排接口。
public interface EventChain<E> {
    default EventChain<E> then(BusinessEventHandler<E>... handlers) {
        return then(null, handlers);
    }

    // 追加广播型 stage。
    EventChain<E> then(String stageName, BusinessEventHandler<E>... handlers);

    default EventChain<E> thenWorkerPool(int workerCount, WorkHandler<E> handler) {
        return thenWorkerPool(null, workerCount, handler);
    }

    // 追加 worker pool stage。
    EventChain<E> thenWorkerPool(String stageName, int workerCount, WorkHandler<E> handler);
}
