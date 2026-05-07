package com.dis.handler;

public final class FatalExceptionHandler<E> implements ExceptionHandler<E> {
    @Override
    public void handleEventException(Throwable ex, long sequence, E event) {
        // 致命异常处理：记录错误上下文，避免线程静默退出。
        System.err.println("FATAL: seq=" + sequence + " event=" + event);
        ex.printStackTrace();
        // 如需强制停机，可在此抛出 RuntimeException。
        // throw new RuntimeException(ex);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        ex.printStackTrace();
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        ex.printStackTrace();
    }
}
