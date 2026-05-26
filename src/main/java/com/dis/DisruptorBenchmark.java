package com.dis;

import com.dis.runtime.DefaultEventEngine;
import com.dis.runtime.EngineConfig;

import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

// 简化版性能对比：本引擎 vs ArrayBlockingQueue。
public class DisruptorBenchmark {
    private static final int EVENT_COUNT = 200;
    private static final int BUFFER_SIZE = 65_536;
    private static final int[] PRODUCER_COUNTS = {1, 4};

    public static void main(String[] args) throws Exception {
        System.out.println("事件数=" + EVENT_COUNT + "，缓冲区大小=" + BUFFER_SIZE);
        for (int producerCount : PRODUCER_COUNTS) {
            BenchmarkReport engineReport = runEngineScenario(producerCount);
            BenchmarkReport queueReport = runBlockingQueueScenario(producerCount);
            System.out.println(formatReport("事件引擎", producerCount, engineReport));
            System.out.println(formatReport("数组阻塞队列", producerCount, queueReport));
            System.out.println("----");
        }
    }

    private static BenchmarkReport runEngineScenario(int producerCount) throws Exception {
        CountDownLatch consumedLatch = new CountDownLatch(EVENT_COUNT);
        long[] latencies = new long[EVENT_COUNT];
        AtomicInteger latencyIndex = new AtomicInteger();

        DefaultEventEngine<BenchmarkEvent> engine = DefaultEventEngine.create(
                EngineConfig.<BenchmarkEvent>builder(BenchmarkEvent::new)
                        .bufferSize(BUFFER_SIZE)
                        .eventResetter(BenchmarkEvent::reset)
                        .observabilityLogEnabled(false)
                        .build()
        );

        engine.handleEventsWith("基准测试阶段", (event, sequence) -> {
            long latency = System.nanoTime() - event.publishedAtNanos;
            int index = latencyIndex.getAndIncrement();
            latencies[index] = latency;
            consumedLatch.countDown();
        });

        engine.start();
        ExecutorService producerPool = Executors.newFixedThreadPool(producerCount);
        int perProducer = EVENT_COUNT / producerCount;
        long start = System.nanoTime();
        for (int i = 0; i < producerCount; i++) {
            producerPool.submit(() -> {
                for (int j = 0; j < perProducer; j++) {
                    engine.publisher().publishEvent((event, sequence) -> event.publishedAtNanos = System.nanoTime());
                }
            });
        }

        producerPool.shutdown();
        producerPool.awaitTermination(10, TimeUnit.MINUTES);
        consumedLatch.await(10, TimeUnit.MINUTES);
        long elapsedNanos = System.nanoTime() - start;

        boolean drained = engine.shutdownGracefully(30, TimeUnit.SECONDS);
        if (!drained) {
            engine.shutdown();
        }
        engine.awaitShutdown(30, TimeUnit.SECONDS);
        return new BenchmarkReport(elapsedNanos, latencies);
    }

    private static BenchmarkReport runBlockingQueueScenario(int producerCount) throws Exception {
        CountDownLatch consumedLatch = new CountDownLatch(EVENT_COUNT);
        long[] latencies = new long[EVENT_COUNT];
        AtomicInteger latencyIndex = new AtomicInteger();
        ArrayBlockingQueue<BenchmarkEvent> queue = new ArrayBlockingQueue<>(BUFFER_SIZE);

        ExecutorService consumer = Executors.newSingleThreadExecutor();
        consumer.submit(() -> {
            try {
                while (consumedLatch.getCount() > 0) {
                    BenchmarkEvent event = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        continue;
                    }
                    int index = latencyIndex.getAndIncrement();
                    latencies[index] = System.nanoTime() - event.publishedAtNanos;
                    consumedLatch.countDown();
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        });

        ExecutorService producerPool = Executors.newFixedThreadPool(producerCount);
        int perProducer = EVENT_COUNT / producerCount;
        long start = System.nanoTime();
        for (int i = 0; i < producerCount; i++) {
            producerPool.submit(() -> {
                for (int j = 0; j < perProducer; j++) {
                    try {
                        queue.put(BenchmarkEvent.of(System.nanoTime()));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
        }

        producerPool.shutdown();
        producerPool.awaitTermination(10, TimeUnit.MINUTES);
        consumedLatch.await(10, TimeUnit.MINUTES);
        long elapsedNanos = System.nanoTime() - start;

        consumer.shutdownNow();
        consumer.awaitTermination(1, TimeUnit.MINUTES);
        producerPool.shutdownNow();
        producerPool.awaitTermination(1, TimeUnit.MINUTES);

        return new BenchmarkReport(elapsedNanos, latencies);
    }

    private static String formatReport(String name, int producerCount, BenchmarkReport report) {
        return String.format(
                "%s | 生产者数量=%d | 吞吐量=%.2f 次/秒 | P99=%.2f 微秒 | 平均=%.2f 微秒",
                name,
                producerCount,
                report.throughput(),
                report.p99Micros(),
                report.avgMicros()
        );
    }

    private static final class BenchmarkReport {
        private final long elapsedNanos;
        private final long[] latencies;

        private BenchmarkReport(long elapsedNanos, long[] latencies) {
            this.elapsedNanos = elapsedNanos;
            this.latencies = latencies;
        }

        private double throughput() {
            return EVENT_COUNT * 1_000_000_000.0 / Math.max(1L, elapsedNanos);
        }

        private double avgMicros() {
            return Arrays.stream(latencies).average().orElse(0.0) / 1_000.0;
        }

        private double p99Micros() {
            long[] copy = Arrays.copyOf(latencies, latencies.length);
            Arrays.sort(copy);
            int index = Math.min(copy.length - 1, (int) Math.ceil(copy.length * 0.99) - 1);
            return copy[index] / 1_000.0;
        }
    }

    public static final class BenchmarkEvent {
        private long publishedAtNanos;

        private BenchmarkEvent() {
        }

        private static BenchmarkEvent of(long publishedAtNanos) {
            BenchmarkEvent event = new BenchmarkEvent();
            event.publishedAtNanos = publishedAtNanos;
            return event;
        }

        private void reset() {
            publishedAtNanos = 0L;
        }
    }
}
