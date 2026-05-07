package com.dis.demo;

import com.dis.OrderEvent;
import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.runtime.DefaultEventEngine;
import com.dis.runtime.EngineConfig;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

public class EngineQuickStart {
    
    public static void main(String[] args) throws Exception {
        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(1024)
                        .observabilityLogIntervalSeconds(5)
                        .build()
        );

        engine.handleEventsWith((event, sequence) -> event.setPrice(sequence))
                .then((event, sequence) -> {
                    if (sequence < 3) {
                        System.out.println("processed=" + sequence + " " + event);
                    }
                });

        engine.start();
        for (int i = 0; i < 10; i++) {
            final int idx = i;
            engine.publisher().publishEvent((event, sequence) -> event.setValues("ORD-" + idx, idx));
        }

        EngineMetricsSnapshot metrics = engine.metricsSnapshot();
        EngineHealthReport health = engine.healthReport();
        System.out.println("metrics.globalLag=" + metrics.globalLag());
        System.out.println("health.level=" + health.level());

        Thread.sleep(500);
        engine.shutdown();
        engine.awaitShutdown(3, TimeUnit.SECONDS);
    }
}
