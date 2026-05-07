package com.dis.handler;

public interface ExceptionHandler<E> {
    void handleEventException(Throwable ex, long sequence, E event);
    void handleOnStartException(Throwable ex);
    void handleOnShutdownException(Throwable ex);
}
