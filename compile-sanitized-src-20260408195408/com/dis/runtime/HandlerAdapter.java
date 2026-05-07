package com.dis.runtime;

import com.dis.api.BusinessEventHandler;
import com.dis.handler.EventHandler;

final class HandlerAdapter<E> implements EventHandler<E> {
    private final BusinessEventHandler<E> delegate;

    HandlerAdapter(BusinessEventHandler<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onEvent(E event, long sequence) throws Exception {
        delegate.onEvent(event, sequence);
    }
}
