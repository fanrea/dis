package com.dis.strategy;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

// 线程池任务拒绝兜底策略示例。
// 当前实现是 no-op，避免在拒绝路径引入额外阻塞。
// 生产场景建议接入告警、落盘或备用队列。
public class UltimateFallbackRejectionPolicy implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        // 故意留空。
    }
}
