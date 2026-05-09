package com.dis.demo;

import com.dis.OrderEvent;
import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.runtime.DefaultEventEngine;
import com.dis.runtime.EngineConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class EngineQuickStart {
    
    public static void main(String[] args) throws Exception {
        CountDownLatch processed = new CountDownLatch(10);
        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(1024)
                        .eventResetter(OrderEvent::reset)
                        .observabilityLogIntervalSeconds(5)
                        .build()
        );

        engine.handleEventsWith("validate-order", (event, sequence) -> {
                    event.setOrderId("ORD-" + sequence);
                    event.setStatus("VALIDATED");
                    event.setCreatedAtMillis(System.currentTimeMillis());
                })
                .then("enrich-order", (event, sequence) -> {
                    event.setPrice(sequence);
                    if (sequence < 3) {
                        System.out.println("processed=" + sequence + " " + event);
                    }
                    processed.countDown();
                });

        engine.start();
        for (int i = 0; i < 10; i++) {
            final int idx = i;
            engine.publisher().publishEvent((event, sequence) -> {
                event.setOrderId("ORD-" + idx);
                event.setUserId("USER-" + idx);
                event.setSkuId("SKU-" + idx);
                event.setQuantity(idx + 1L);
                event.setTraceId("trace-" + idx);
                event.setRiskPassed(true);
                event.setStockReserved(true);
                event.setStatus("NEW");
                event.setCreatedAtMillis(System.currentTimeMillis());
            });
        }

        processed.await(3, TimeUnit.SECONDS);
        EngineMetricsSnapshot metrics = engine.metricsSnapshot();
        EngineHealthReport health = engine.healthReport();
        System.out.println("metrics.globalLag=" + metrics.globalLag());
        System.out.println("health.level=" + health.level());

        engine.shutdownGracefully(3, TimeUnit.SECONDS);
    }
}
