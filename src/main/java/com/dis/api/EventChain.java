package com.dis.api;

public interface EventChain<E> {
    EventChain<E> then(BusinessEventHandler<E>... handlers);
}
