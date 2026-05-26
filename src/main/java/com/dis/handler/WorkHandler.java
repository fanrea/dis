package com.dis.handler;

// WorkerPool 单事件处理接口。
public interface WorkHandler<T> {
    void onEvent(T event) throws Exception;
}
