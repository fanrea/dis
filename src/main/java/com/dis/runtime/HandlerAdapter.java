package com.dis.runtime;

import com.dis.api.BusinessEventHandler;
import com.dis.handler.EventHandler;

// 将上层 BusinessEventHandler 适配为核心处理器使用的 EventHandler。
// runtime 层负责 API 和 core 的隔离，core 只认识更底层的 EventHandler 接口。
final class HandlerAdapter<E> implements EventHandler<E> {
    private final BusinessEventHandler<E> delegate; // 用户传入的业务 handler。

    HandlerAdapter(BusinessEventHandler<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onEvent(E event, long sequence) throws Exception {
        // 不改变业务语义，只把事件和 sequence 原样转发给用户 handler。
        delegate.onEvent(event, sequence);
    }
}
