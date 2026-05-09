package com.dis.handler;

import com.dis.OrderEvent;

import java.util.concurrent.CountDownLatch;

// 压测用事件处理器。
public class BenchmarkEventHandler implements EventHandler<OrderEvent> {
    private final CountDownLatch completeLatch;
    private final long targetCount;
    private long processedCount = 0;

    public BenchmarkEventHandler(CountDownLatch completeLatch, long targetCount) {
        this.completeLatch = completeLatch;
        this.targetCount = targetCount;
    }

    @Override
    public void onEvent(OrderEvent event, long sequence) {
        processedCount++;
        event.setPrice(processedCount);

        if (processedCount == targetCount) {
            System.out.println("Processed " + processedCount + " events");
            completeLatch.countDown();
        }
    }
}
