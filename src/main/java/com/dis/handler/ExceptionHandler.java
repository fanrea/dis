package com.dis.handler;

// 引擎异常处理回调。
public interface ExceptionHandler<E> {
    void handleEventException(Throwable ex, long sequence, E event);

    void handleOnStartException(Throwable ex);

    void handleOnShutdownException(Throwable ex);
}
