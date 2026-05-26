package com.dis.handler;

// 底层消费者处理接口。
// 与 BusinessEventHandler 语义接近，主要供核心处理器使用。
public interface EventHandler<E> {
    void onEvent(E event, long sequence) throws Exception;
}
