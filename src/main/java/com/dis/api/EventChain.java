package com.dis.api;

import com.dis.handler.WorkHandler;

public interface EventChain<E> {
    default EventChain<E> then(BusinessEventHandler<E>... handlers) {
        return then(null, handlers);
    }

    EventChain<E> then(String stageName, BusinessEventHandler<E>... handlers);

    default EventChain<E> thenWorkerPool(int workerCount, WorkHandler<E> handler) {
        return thenWorkerPool(null, workerCount, handler);
    }

    EventChain<E> thenWorkerPool(String stageName, int workerCount, WorkHandler<E> handler);
}
