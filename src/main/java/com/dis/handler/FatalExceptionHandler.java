package com.dis.handler;

// 默认异常处理器。
// 当前策略：打印错误上下文，保持线程不被静默终止。
public final class FatalExceptionHandler<E> implements ExceptionHandler<E> {
    @Override
    public void handleEventException(Throwable ex, long sequence, E event) {
        System.err.println("严重异常：序号=" + sequence + "，事件=" + event);
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
