package com.dis.handler;

import com.dis.strategy.UltimateFallbackRejectionPolicy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// 异步死信异常处理器示例。
// 设计意图：
// 1. 主消费线程只负责抛转，避免在异常路径上做重 IO。
// 2. 通过独立线程池异步处理死信落地。
public class DlqExceptionHandler<E> implements ExceptionHandler<E> {
    private final ExecutorService executor;

    public DlqExceptionHandler() {
        this.executor = new ThreadPoolExecutor(
                2,
                4,
                60L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(1000),
                new ThreadFactory() {
                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r, "dlq-handler-" + threadNumber.getAndIncrement());
                        thread.setDaemon(true);
                        return thread;
                    }
                },
                new UltimateFallbackRejectionPolicy()
        );
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, E event) {
        System.err.println("死信异常，序号=" + sequence + "，事件=" + event);
        executor.submit(() -> {
            // 待实现：将异常事件异步发送到消息队列或数据库死信表。
        });
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        System.err.println("死信处理器启动异常");
        ex.printStackTrace();
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        System.err.println("死信处理器关闭异常");
        ex.printStackTrace();
    }
}
