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

// 完整流水线示例。
// 演示点：
// 1. 多 stage 链路 + WorkerPool。
// 2. translator 失败跳过。
// 3. retry + dead letter。
// 4. tryPublish 超时。
@SuppressWarnings("all")
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

        engine.handleEventsWith("校验订单", (event, sequence) -> {
                    if (isBlank(event.getOrderId()) || isBlank(event.getUserId()) || isBlank(event.getSkuId())) {
                        event.setOrderStatus(OrderEventStatus.FAILED);
                        event.setErrorCode("订单无效");
                        event.setErrorMessage("订单字段缺失");
                        throw new IllegalStateException("订单字段缺失");
                    }
                    if (event.getPrice() <= 0 || event.getQuantity() <= 0) {
                        event.setOrderStatus(OrderEventStatus.FAILED);
                        event.setErrorCode("订单无效");
                        event.setErrorMessage("价格或数量不合法");
                        throw new IllegalStateException("价格或数量不合法");
                    }
                    event.setOrderStatus(OrderEventStatus.VALIDATED);
                    System.out.println("[校验订单] " + event.getOrderId() + " 通过");
                })
                .then("风控检查", (event, sequence) -> {
                    AtomicInteger attempt = riskAttempts.computeIfAbsent(event.getOrderId(), key -> new AtomicInteger());
                    if ("ORD-RETRY".equals(event.getOrderId()) && attempt.incrementAndGet() < 3) {
                        event.setOrderStatus(OrderEventStatus.RISK_REJECTED);
                        event.setErrorCode("风控临时异常");
                        event.setErrorMessage("风控服务超时");
                        throw new IllegalStateException("风控服务超时");
                    }
                    if ("BLOCKED".equals(event.getUserId())) {
                        event.setOrderStatus(OrderEventStatus.RISK_REJECTED);
                        event.setErrorCode("风控拒绝");
                        event.setErrorMessage("风控规则拒绝该用户");
                        throw new IllegalStateException("风控规则拒绝该用户");
                    }
                    event.setRiskPassed(true);
                    event.setOrderStatus(OrderEventStatus.RISK_PASSED);
                    event.setErrorCode(null);
                    event.setErrorMessage(null);
                    System.out.println("[风控检查] " + event.getOrderId() + " 通过");
                })
                .thenWorkerPool("预占库存", 2, event -> {
                    AtomicInteger attempt = stockAttempts.computeIfAbsent(event.getOrderId(), key -> new AtomicInteger());
                    if ("ORD-DLQ".equals(event.getOrderId())) {
                        event.setOrderStatus(OrderEventStatus.STOCK_FAILED);
                        event.setErrorCode("库存不足");
                        event.setErrorMessage("库存不足");
                        throw new IllegalStateException("库存不足");
                    }
                    if (!event.isRiskPassed()) {
                        event.setOrderStatus(OrderEventStatus.STOCK_FAILED);
                        event.setErrorCode("风控未通过");
                        event.setErrorMessage("风控未通过");
                        throw new IllegalStateException("风控未通过");
                    }
                    if ("ORD-RETRY".equals(event.getOrderId()) && attempt.incrementAndGet() == 1) {
                        event.setOrderStatus(OrderEventStatus.STOCK_FAILED);
                        event.setErrorCode("库存临时异常");
                        event.setErrorMessage("库存预占繁忙");
                        throw new IllegalStateException("库存预占繁忙");
                    }
                    event.setStockReserved(true);
                    event.setOrderStatus(OrderEventStatus.STOCK_RESERVED);
                    event.setErrorCode(null);
                    event.setErrorMessage(null);
                    System.out.println("[预占库存] " + event.getOrderId() + " 已预占");
                })
                .then("发送通知", (event, sequence) -> {
                    if (event.isRiskPassed() && event.isStockReserved()) {
                        event.setOrderStatus(OrderEventStatus.NOTIFIED);
                        System.out.println("[发送通知] " + event.getOrderId() + " 已通知");
                    } else {
                        if (event.getOrderStatus() == null) {
                            event.setOrderStatus(OrderEventStatus.FAILED);
                        }
                        System.out.println("[发送通知] " + event.getOrderId()
                                + " 已跳过，状态=" + event.getOrderStatus()
                                + "，错误码=" + event.getErrorCode());
                    }
                    pipelineFinished.countDown();
                });

        engine.start();

        publishOrder(engine, "ORD-1001", "USER-1", "SKU-1", 199, 1, "trace-1");
        try {
            engine.publisher().publishEvent((event, sequence) -> {
                event.setOrderId("ORD-PUBLISH-FAILED");
                throw new IllegalStateException("发布转换器在订单就绪前失败");
            });
        } catch (IllegalStateException ex) {
            System.out.println("[发布失败] " + ex.getMessage());
        }
        publishOrder(engine, "ORD-RETRY", "USER-2", "SKU-2", 299, 2, "trace-2");
        publishOrder(engine, "ORD-DLQ", "USER-3", "SKU-3", 399, 1, "trace-3");
        publishOrder(engine, "ORD-1002", "BLOCKED", "SKU-4", 499, 1, "trace-4");

        pipelineFinished.await(3, TimeUnit.SECONDS);

        boolean drained = engine.shutdownGracefully(5, TimeUnit.SECONDS);
        System.out.println("优雅停机结果=" + drained);

        EngineMetricsSnapshot metrics = engine.metricsSnapshot();
        EngineHealthReport health = engine.healthReport();
        System.out.println("指标.发布转换失败次数=" + metrics.publishTranslateFailedCount());
        System.out.println("指标.发布超时次数=" + metrics.publishTimeoutCount());
        System.out.println("指标.消费者跳过转换失败次数=" + metrics.consumerSkippedTranslateFailedCount());
        System.out.println("指标.处理器重试次数=" + metrics.handlerRetryCount());
        System.out.println("指标.死信次数=" + metrics.deadLetterCount());
        System.out.println("指标.优雅停机次数=" + metrics.gracefulShutdownCount());
        System.out.println("指标.优雅停机超时次数=" + metrics.gracefulShutdownTimeoutCount());
        System.out.println("健康状态=" + health.level() + " | " + health.summary());

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
        tinyEngine.handleEventsWith("慢阶段", (event, sequence) -> {
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
        System.out.println("限时发布是否成功=" + accepted);
        System.out.println("限时发布超时指标=" + tinyEngine.metricsSnapshot().publishTimeoutCount());
        release.countDown();
        tinyEngine.shutdownGracefully(3, TimeUnit.SECONDS);
    }

    private static void logDeadLetter(DeadLetterEvent<OrderEvent> event) {
        System.out.println("[死信] 阶段=" + event.stageName()
                + "，序号=" + event.sequence()
                + "，订单ID=" + (event.eventSnapshotOrReference() == null ? null : event.eventSnapshotOrReference().getOrderId())
                + "，原因=" + event.cause().getMessage());
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static final class DemoExceptionHandler implements ExceptionHandler<OrderEvent> {
        @Override
        public void handleEventException(Throwable ex, long sequence, OrderEvent event) {
            System.out.println("[处理器异常] 序号=" + sequence
                    + "，订单ID=" + (event == null ? null : event.getOrderId())
                    + "，原因=" + ex.getMessage());
        }

        @Override
        public void handleOnStartException(Throwable ex) {
            System.out.println("[启动异常] " + ex.getMessage());
        }

        @Override
        public void handleOnShutdownException(Throwable ex) {
            System.out.println("[关闭异常] " + ex.getMessage());
        }
    }
}
