package com.dis.demo;

import com.dis.OrderEvent;
import com.dis.OrderEventStatus;
import com.dis.api.DeadLetterEvent;
import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.api.RetryPolicy;
import com.dis.handler.ExceptionHandler;
import com.dis.runtime.DefaultEventEngine;
import com.dis.runtime.EngineConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class OrderPipelineDemo {
    private OrderPipelineDemo() {
    }

    public static void main(String[] args) throws Exception {
        CountDownLatch pipelineFinished = new CountDownLatch(4);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(1024)
                        .eventResetter(OrderEvent::reset)
                        .retryPolicy(RetryPolicy.fixedBackoff(3, 10))
                        .exceptionHandler(new DemoExceptionHandler())
                        .observabilityLogEnabled(true)
                        .observabilityLogIntervalSeconds(1)
                        .deadLetterHandler(OrderPipelineDemo::logDeadLetter)
                        .build()
        );

        Map<String, AtomicInteger> riskAttempts = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> stockAttempts = new ConcurrentHashMap<>();

        engine.handleEventsWith("validate-order", (event, sequence) -> {
                    if (isBlank(event.getOrderId()) || isBlank(event.getUserId()) || isBlank(event.getSkuId())) {
                        event.setOrderStatus(OrderEventStatus.FAILED);
                        event.setErrorCode("INVALID_ORDER");
                        event.setErrorMessage("missing order fields");
                        throw new IllegalStateException("missing order fields");
                    }
                    if (event.getPrice() <= 0 || event.getQuantity() <= 0) {
                        event.setOrderStatus(OrderEventStatus.FAILED);
                        event.setErrorCode("INVALID_ORDER");
                        event.setErrorMessage("price or quantity is invalid");
                        throw new IllegalStateException("price or quantity is invalid");
                    }
                    event.setOrderStatus(OrderEventStatus.VALIDATED);
                    System.out.println("[validate-order] " + event.getOrderId() + " ok");
                })
                .then("risk-check", (event, sequence) -> {
                    AtomicInteger attempt = riskAttempts.computeIfAbsent(event.getOrderId(), key -> new AtomicInteger());
                    if ("ORD-RETRY".equals(event.getOrderId()) && attempt.incrementAndGet() < 3) {
                        event.setOrderStatus(OrderEventStatus.RISK_REJECTED);
                        event.setErrorCode("RISK_TEMP");
                        event.setErrorMessage("risk service timeout");
                        throw new IllegalStateException("risk service timeout");
                    }
                    if ("BLOCKED".equals(event.getUserId())) {
                        event.setOrderStatus(OrderEventStatus.RISK_REJECTED);
                        event.setErrorCode("RISK_BLOCKED");
                        event.setErrorMessage("risk rule rejected user");
                        throw new IllegalStateException("risk rule rejected user");
                    }
                    event.setRiskPassed(true);
                    event.setOrderStatus(OrderEventStatus.RISK_PASSED);
                    event.setErrorCode(null);
                    event.setErrorMessage(null);
                    System.out.println("[risk-check] " + event.getOrderId() + " passed");
                })
                .thenWorkerPool("reserve-stock", 2, event -> {
                    AtomicInteger attempt = stockAttempts.computeIfAbsent(event.getOrderId(), key -> new AtomicInteger());
                    if ("ORD-DLQ".equals(event.getOrderId())) {
                        event.setOrderStatus(OrderEventStatus.STOCK_FAILED);
                        event.setErrorCode("STOCK_EMPTY");
                        event.setErrorMessage("stock exhausted");
                        throw new IllegalStateException("stock exhausted");
                    }
                    if (!event.isRiskPassed()) {
                        event.setOrderStatus(OrderEventStatus.STOCK_FAILED);
                        event.setErrorCode("RISK_NOT_PASSED");
                        event.setErrorMessage("risk check not passed");
                        throw new IllegalStateException("risk check not passed");
                    }
                    if ("ORD-RETRY".equals(event.getOrderId()) && attempt.incrementAndGet() == 1) {
                        event.setOrderStatus(OrderEventStatus.STOCK_FAILED);
                        event.setErrorCode("STOCK_TEMP");
                        event.setErrorMessage("stock reservation busy");
                        throw new IllegalStateException("stock reservation busy");
                    }
                    event.setStockReserved(true);
                    event.setOrderStatus(OrderEventStatus.STOCK_RESERVED);
                    event.setErrorCode(null);
                    event.setErrorMessage(null);
                    System.out.println("[reserve-stock] " + event.getOrderId() + " reserved");
                })
                .then("send-notification", (event, sequence) -> {
                    if (event.isRiskPassed() && event.isStockReserved()) {
                        event.setOrderStatus(OrderEventStatus.NOTIFIED);
                        System.out.println("[send-notification] " + event.getOrderId() + " notified");
                    } else {
                        if (event.getOrderStatus() == null) {
                            event.setOrderStatus(OrderEventStatus.FAILED);
                        }
                        System.out.println("[send-notification] " + event.getOrderId()
                                + " skipped, status=" + event.getOrderStatus()
                                + ", error=" + event.getErrorCode());
                    }
                    pipelineFinished.countDown();
                });

        engine.start();

        publishOrder(engine, "ORD-1001", "USER-1", "SKU-1", 199, 1, "trace-1");
        try {
            engine.publisher().publishEvent((event, sequence) -> {
                event.setOrderId("ORD-PUBLISH-FAILED");
                throw new IllegalStateException("translator failed before order is ready");
            });
        } catch (IllegalStateException ex) {
            System.out.println("[publish-failed] " + ex.getMessage());
        }
        publishOrder(engine, "ORD-RETRY", "USER-2", "SKU-2", 299, 2, "trace-2");
        publishOrder(engine, "ORD-DLQ", "USER-3", "SKU-3", 399, 1, "trace-3");
        publishOrder(engine, "ORD-1002", "BLOCKED", "SKU-4", 499, 1, "trace-4");

        pipelineFinished.await(3, TimeUnit.SECONDS);

        boolean drained = engine.shutdownGracefully(5, TimeUnit.SECONDS);
        System.out.println("shutdownGracefully=" + drained);

        EngineMetricsSnapshot metrics = engine.metricsSnapshot();
        EngineHealthReport health = engine.healthReport();
        System.out.println("metrics.publishTranslateFailedCount=" + metrics.publishTranslateFailedCount());
        System.out.println("metrics.publishTimeoutCount=" + metrics.publishTimeoutCount());
        System.out.println("metrics.consumerSkippedTranslateFailedCount=" + metrics.consumerSkippedTranslateFailedCount());
        System.out.println("metrics.handlerRetryCount=" + metrics.handlerRetryCount());
        System.out.println("metrics.deadLetterCount=" + metrics.deadLetterCount());
        System.out.println("metrics.gracefulShutdownCount=" + metrics.gracefulShutdownCount());
        System.out.println("metrics.gracefulShutdownTimeoutCount=" + metrics.gracefulShutdownTimeoutCount());
        System.out.println("health=" + health.level() + " | " + health.summary());

        demoTryPublishTimeout();
    }

    private static void publishOrder(DefaultEventEngine<OrderEvent> engine,
                                     String orderId,
                                     String userId,
                                     String skuId,
                                     long price,
                                     long quantity,
                                     String traceId) {
        engine.publisher().publishEvent((event, sequence) -> {
            event.setOrderId(orderId);
            event.setUserId(userId);
            event.setSkuId(skuId);
            event.setPrice(price);
            event.setQuantity(quantity);
            event.setTraceId(traceId);
            event.setCreatedAtMillis(System.currentTimeMillis());
            event.setOrderStatus(OrderEventStatus.NEW);
            event.setRiskPassed(false);
            event.setStockReserved(false);
            event.setErrorCode(null);
            event.setErrorMessage(null);
        });
    }

    private static void demoTryPublishTimeout() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        DefaultEventEngine<OrderEvent> tinyEngine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(1)
                        .eventResetter(OrderEvent::reset)
                        .observabilityLogEnabled(false)
                        .build()
        );
        tinyEngine.handleEventsWith("slow-stage", (event, sequence) -> {
            entered.countDown();
            release.await(3, TimeUnit.SECONDS);
        });
        tinyEngine.start();
        tinyEngine.publisher().publishEvent((event, sequence) -> event.setOrderId("TINY-1"));
        entered.await(3, TimeUnit.SECONDS);
        boolean accepted = tinyEngine.publisher().tryPublishEvent(
                (event, sequence) -> event.setOrderId("TINY-2"),
                10,
                TimeUnit.MILLISECONDS
        );
        System.out.println("limitedPublishAccepted=" + accepted);
        System.out.println("limitedPublishTimeoutMetric=" + tinyEngine.metricsSnapshot().publishTimeoutCount());
        release.countDown();
        tinyEngine.shutdownGracefully(3, TimeUnit.SECONDS);
    }

    private static void logDeadLetter(DeadLetterEvent<OrderEvent> event) {
        System.out.println("[dead-letter] stage=" + event.stageName()
                + ", seq=" + event.sequence()
                + ", orderId=" + (event.eventSnapshotOrReference() == null ? null : event.eventSnapshotOrReference().getOrderId())
                + ", cause=" + event.cause().getMessage());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static final class DemoExceptionHandler implements ExceptionHandler<OrderEvent> {
        @Override
        public void handleEventException(Throwable ex, long sequence, OrderEvent event) {
            System.out.println("[handler-error] seq=" + sequence
                    + ", orderId=" + (event == null ? null : event.getOrderId())
                    + ", cause=" + ex.getMessage());
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            System.out.println("[start-error] " + ex.getMessage());
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            System.out.println("[shutdown-error] " + ex.getMessage());
        }
    }
}
