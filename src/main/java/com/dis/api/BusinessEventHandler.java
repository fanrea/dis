package com.dis.api;

// 业务 handler 抽象。
// 注意：
// 1. sequence 是引擎内逻辑序号，可用于日志定位。
// 2. 抛出异常会进入重试/死信流程（取决于 RetryPolicy）。
@FunctionalInterface
public interface BusinessEventHandler<E> {
    void onEvent(E event, long sequence) throws Exception;
}
