package com.dis.runtime;

import com.dis.OrderEvent;
import com.dis.api.EventPublisher;
import com.dis.core.MultiProducerSequencer;
import com.dis.core.Sequence;
import com.dis.strategy.YieldingWaitStrategy;
import com.dis.api.DeadLetterEvent;
import com.dis.api.RetryPolicy;
import com.dis.handler.ExceptionHandler;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultEventEngineTest {

    @Test
    void publishFailureDoesNotPoisonSubsequentEvents() throws Exception {
        List<OrderEvent> seen = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(1);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(4)
                        .eventResetter(OrderEvent::reset)
                        .exceptionHandler(noOpExceptionHandler())
                        .deadLetterHandler(event -> {
                        })
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("order-stage", (event, sequence) -> {
            synchronized (seen) {
                seen.add(copy(event));
            }
            latch.countDown();
        });

        engine.start();

        assertThrows(IllegalStateException.class, () ->
                engine.publisher().publishEvent((event, sequence) -> {
                    event.setOrderId("bad");
                    throw new IllegalStateException("boom");
                })
        );

        engine.publisher().publishEvent((event, sequence) -> {
            event.setOrderId("good");
            event.setUserId("u-1");
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        engine.shutdownGracefully(3, TimeUnit.SECONDS);

        synchronized (seen) {
            assertEquals(1, seen.size());
            assertEquals("good", seen.get(0).getOrderId());
            assertEquals("u-1", seen.get(0).getUserId());
        }
        assertEquals(1, engine.metricsSnapshot().publishTranslateFailedCount());
        assertEquals(1, engine.metricsSnapshot().consumerSkippedTranslateFailedCount());
    }

    @Test
    void tryNextRespectsTimeoutWhenGateDoesNotAdvance() {
        MultiProducerSequencer sequencer = new MultiProducerSequencer(1, new YieldingWaitStrategy());
        Sequence gate = new Sequence(-1);
        sequencer.addGatingSequence(gate);

        long first = sequencer.next();
        assertEquals(0L, first);

        long second = sequencer.tryNext(10, TimeUnit.MILLISECONDS);
        assertEquals(-1L, second);
    }

    @Test
    void tryPublishEventReturnsFalseAndRecordsTimeout() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(1)
                        .eventResetter(OrderEvent::reset)
                        .exceptionHandler(noOpExceptionHandler())
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("slow-stage", (event, sequence) -> {
            entered.countDown();
            release.await(3, TimeUnit.SECONDS);
        });

        engine.start();
        engine.publisher().publishEvent((event, sequence) -> event.setOrderId("first"));
        assertTrue(entered.await(3, TimeUnit.SECONDS));

        boolean published = engine.publisher().tryPublishEvent(
                (event, sequence) -> event.setOrderId("timeout"),
                10,
                TimeUnit.MILLISECONDS
        );

        assertFalse(published);
        assertEquals(1, engine.metricsSnapshot().publishTimeoutCount());

        release.countDown();
        assertTrue(engine.shutdownGracefully(3, TimeUnit.SECONDS));
    }

    @Test
    void gracefulShutdownRejectsNewPublishes() throws Exception {
        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(4)
                        .eventResetter(OrderEvent::reset)
                        .exceptionHandler(noOpExceptionHandler())
                        .deadLetterHandler(event -> {
                        })
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("noop-stage", (event, sequence) -> {
        });

        engine.start();
        assertTrue(engine.shutdownGracefully(3, TimeUnit.SECONDS));

        assertThrows(IllegalStateException.class, () ->
                engine.publisher().publishEvent((event, sequence) -> event.setOrderId("after-shutdown"))
        );
    }

    @Test
    void eventResetterClearsStaleFieldsOnReuse() throws Exception {
        List<OrderEvent> seen = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(1)
                        .eventResetter(OrderEvent::reset)
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("reset-stage", (event, sequence) -> {
            synchronized (seen) {
                seen.add(copy(event));
            }
            latch.countDown();
        });

        engine.start();
        engine.publisher().publishEvent((event, sequence) -> {
            event.setOrderId("first");
            event.setUserId("user-a");
            event.setSkuId("sku-a");
        });
        engine.publisher().publishEvent((event, sequence) -> {
            event.setOrderId("second");
        });

        assertTrue(latch.await(3, TimeUnit.SECONDS));
        engine.shutdownGracefully(3, TimeUnit.SECONDS);
        assertNotNull(seen.get(0));
        assertNotNull(seen.get(1));

        synchronized (seen) {
            assertEquals("first", seen.get(0).getOrderId());
            assertEquals("user-a", seen.get(0).getUserId());
            assertEquals("sku-a", seen.get(0).getSkuId());
            assertEquals("second", seen.get(1).getOrderId());
            assertEquals(null, seen.get(1).getUserId());
            assertEquals(null, seen.get(1).getSkuId());
        }
    }

    @Test
    void retryPolicyRetriesUntilSuccess() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<DeadLetterEvent<OrderEvent>> deadLetter = new AtomicReference<>();
        CountDownLatch processed = new CountDownLatch(1);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(4)
                        .eventResetter(OrderEvent::reset)
                        .retryPolicy(RetryPolicy.fixedBackoff(3, 0L))
                        .deadLetterHandler(deadLetter::set)
                        .exceptionHandler(noOpExceptionHandler())
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("retry-stage", (event, sequence) -> {
            if (attempts.incrementAndGet() < 2) {
                throw new IllegalStateException("retry");
            }
            processed.countDown();
        });

        engine.start();
        engine.publisher().publishEvent((event, sequence) -> event.setOrderId("retry-ok"));

        assertTrue(processed.await(3, TimeUnit.SECONDS));
        engine.shutdownGracefully(3, TimeUnit.SECONDS);

        assertEquals(2, attempts.get());
        assertEquals(null, deadLetter.get());
        assertEquals(1, engine.metricsSnapshot().handlerRetryCount());
    }

    @Test
    void deadLetterReceivesEventAfterRetriesAreExhausted() throws Exception {
        AtomicReference<DeadLetterEvent<OrderEvent>> deadLetter = new AtomicReference<>();
        CountDownLatch deadLetterSeen = new CountDownLatch(1);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(4)
                        .eventResetter(OrderEvent::reset)
                        .retryPolicy(RetryPolicy.fixedBackoff(2, 0L))
                        .deadLetterHandler(event -> {
                            deadLetter.set(event);
                            deadLetterSeen.countDown();
                        })
                        .exceptionHandler(noOpExceptionHandler())
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("dead-letter-stage", (event, sequence) -> {
            throw new IllegalStateException("always fail");
        });

        engine.start();
        engine.publisher().publishEvent((event, sequence) -> event.setOrderId("dlq"));

        assertTrue(deadLetterSeen.await(3, TimeUnit.SECONDS));
        engine.shutdownGracefully(3, TimeUnit.SECONDS);

        DeadLetterEvent<OrderEvent> event = deadLetter.get();
        assertNotNull(event);
        assertEquals("dead-letter-stage-handler-1", event.stageName());
        assertEquals(2, event.attempts());
        assertEquals("dlq", event.eventSnapshotOrReference().getOrderId());
        assertEquals(1, engine.metricsSnapshot().deadLetterCount());
    }

    @Test
    void workerPoolProcessesEachEventOnceAndDownstreamWaitsForPool() throws Exception {
        int eventCount = 24;
        Map<String, AtomicInteger> workerSeen = new ConcurrentHashMap<>();
        AtomicInteger workerTotal = new AtomicInteger();
        AtomicInteger downstreamTotal = new AtomicInteger();
        CountDownLatch downstreamProcessed = new CountDownLatch(eventCount);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(64)
                        .eventResetter(OrderEvent::reset)
                        .exceptionHandler(noOpExceptionHandler())
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWithWorkerPool("reserve-stock", 3, event -> {
                    workerSeen.computeIfAbsent(event.getOrderId(), key -> new AtomicInteger()).incrementAndGet();
                    event.setStockReserved(true);
                    workerTotal.incrementAndGet();
                })
                .then("notify", (event, sequence) -> {
                    if (event.isStockReserved()) {
                        downstreamTotal.incrementAndGet();
                    }
                    downstreamProcessed.countDown();
                });

        engine.start();
        EventPublisher<OrderEvent> publisher = engine.publisher();
        for (int i = 0; i < eventCount; i++) {
            int orderNo = i;
            publisher.publishEvent((event, sequence) -> event.setOrderId("ORD-" + orderNo));
        }

        assertTrue(downstreamProcessed.await(3, TimeUnit.SECONDS));
        assertTrue(engine.shutdownGracefully(3, TimeUnit.SECONDS));

        assertEquals(eventCount, workerTotal.get());
        assertEquals(eventCount, downstreamTotal.get());
        for (int i = 0; i < eventCount; i++) {
            assertEquals(1, workerSeen.get("ORD-" + i).get());
        }
    }

    @Test
    void gracefulShutdownCanTimeoutAndRemainDraining() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        DefaultEventEngine<OrderEvent> engine = DefaultEventEngine.create(
                EngineConfig.<OrderEvent>builder(OrderEvent::new)
                        .bufferSize(4)
                        .eventResetter(OrderEvent::reset)
                        .exceptionHandler(noOpExceptionHandler())
                        .deadLetterHandler(event -> {
                        })
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("drain-stage", (event, sequence) -> {
            entered.countDown();
            release.await(3, TimeUnit.SECONDS);
        });

        engine.start();
        engine.publisher().publishEvent((event, sequence) -> event.setOrderId("drain"));
        assertTrue(entered.await(3, TimeUnit.SECONDS));

        boolean drained = engine.shutdownGracefully(1, TimeUnit.MILLISECONDS);
        assertFalse(drained);
        assertEquals(EngineState.DRAINING, engine.metricsSnapshot().state());

        release.countDown();
        engine.shutdown();
        engine.awaitShutdown(3, TimeUnit.SECONDS);
    }

    private static OrderEvent copy(OrderEvent source) {
        OrderEvent copy = new OrderEvent();
        copy.setOrderId(source.getOrderId());
        copy.setUserId(source.getUserId());
        copy.setSkuId(source.getSkuId());
        copy.setPrice(source.getPrice());
        copy.setQuantity(source.getQuantity());
        copy.setTraceId(source.getTraceId());
        copy.setStatus(source.getStatus());
        copy.setRiskPassed(source.isRiskPassed());
        copy.setStockReserved(source.isStockReserved());
        copy.setCreatedAtMillis(source.getCreatedAtMillis());
        copy.setErrorCode(source.getErrorCode());
        copy.setErrorMessage(source.getErrorMessage());
        return copy;
    }

    private static ExceptionHandler<OrderEvent> noOpExceptionHandler() {
        return new ExceptionHandler<>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, OrderEvent event) {
            }

            @Override
            public void handleOnStartException(Throwable ex) {
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
            }
        };
    }
}
