package com.dis.handler;

import com.dis.OrderEvent;

import java.util.concurrent.CountDownLatch;

// 压测用事件处理器。
public class BenchmarkEventHandler implements EventHandler<OrderEvent> {
    private final CountDownLatch completeLatch;
    private final long targetCount;

    // 记录已处理条数。
    private long processedCount = 0;

    public BenchmarkEventHandler(CountDownLatch completeLatch, long targetCount) {
        this.completeLatch = completeLatch;
        this.targetCount = targetCount;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence) {
        // 这里模拟轻量内存计算逻辑。
        processedCount++;
        event.setPrice(processedCount);

        // 达到目标条数后通知主线程结束压测。
        if (processedCount == targetCount) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Processed " + processedCount + " events");
            completeLatch.countDown();
        }
    }
}
