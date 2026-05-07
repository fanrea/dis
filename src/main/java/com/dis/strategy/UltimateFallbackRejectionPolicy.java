package com.dis.strategy;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class UltimateFallbackRejectionPolicy implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        // 兜底拒绝策略。
        // 建议在生产环境实现为异步落盘、告警上报或转发到备用队列。
        // 这里保留空实现，避免在拒绝路径引入阻塞操作。
    }
}
