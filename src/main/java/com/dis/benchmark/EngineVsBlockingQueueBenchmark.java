package com.dis.benchmark;

import com.dis.runtime.DefaultEventEngine;
import com.dis.runtime.EngineConfig;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// 对比引擎与 BlockingQueue 的基准。
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Threads(1)
public class EngineVsBlockingQueueBenchmark {
    private static final int THROUGHPUT_BATCH = 256;

    @Param({"engine", "queue"})
    private String backend;

    @Param({"1", "2", "4"})
    private int producerCount;

    private ExecutorService producerPool;
    private ExecutorService queueConsumerPool;
    private DefaultEventEngine<BenchmarkEvent> engine;
    private ArrayBlockingQueue<BenchmarkEvent> queue;
    private volatile CountDownLatch batchLatch;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    @Setup
    public void setup() {
        producerPool = Executors.newFixedThreadPool(producerCount, new NamedThreadFactory("jmh-producer"));
        if ("engine".equalsIgnoreCase(backend)) {
            engine = DefaultEventEngine.create(
                    EngineConfig.<BenchmarkEvent>builder(BenchmarkEvent::new)
                            .bufferSize(1024)
                            .eventResetter(BenchmarkEvent::reset)
                            .observabilityLogEnabled(false)
                            .build()
            );
            engine.handleEventsWith("基准测试阶段", (event, sequence) -> onConsumed());
            engine.start();
        } else {
            queue = new ArrayBlockingQueue<>(4096);
            queueConsumerPool = Executors.newSingleThreadExecutor(new NamedThreadFactory("jmh-queue-consumer"));
            queueConsumerPool.submit(this::consumeQueue);
        }
    }

    @TearDown
    public void tearDown() throws InterruptedException {
        if (engine != null) {
            boolean drained = engine.shutdownGracefully(5, TimeUnit.SECONDS);
            if (!drained) {
                engine.shutdown();
            }
            engine.awaitShutdown(5, TimeUnit.SECONDS);
        }

        if (queueConsumerPool != null) {
            queueConsumerPool.shutdownNow();
            queueConsumerPool.awaitTermination(5, TimeUnit.SECONDS);
        }

        if (producerPool != null) {
            producerPool.shutdownNow();
            producerPool.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OperationsPerInvocation(THROUGHPUT_BATCH)
    public void publishThroughputBatch() throws Exception {
        runBatch(THROUGHPUT_BATCH);
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void publishLatencyRoundTrip() throws Exception {
        runBatch(1);
    }

    private void runBatch(int eventCount) throws Exception {
        checkFailure();

        CountDownLatch latch = new CountDownLatch(eventCount);
        batchLatch = latch;

        List<Future<?>> futures = new ArrayList<>(producerCount);
        int base = eventCount / producerCount;
        int remainder = eventCount % producerCount;

        for (int i = 0; i < producerCount; i++) {
            int quota = base + (i < remainder ? 1 : 0);
            futures.add(producerPool.submit(() -> {
                publishEvents(quota);
                return null;
            }));
        }

        if (!latch.await(1, TimeUnit.MINUTES)) {
            throw new IllegalStateException("基准测试超时");
        }

        for (Future<?> future : futures) {
            future.get();
        }

        checkFailure();
    }

    private void publishEvents(int quota) throws Exception {
        for (int i = 0; i < quota; i++) {
            checkFailure();
            long publishedAtNanos = System.nanoTime();
            if (engine != null) {
                engine.publisher().publishEvent((event, sequence) -> event.setPublishedAtNanos(publishedAtNanos));
            } else {
                queue.put(BenchmarkEvent.of(publishedAtNanos));
            }
        }
    }

    private void consumeQueue() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                BenchmarkEvent event = queue.take();
                onConsumed();
                event.reset();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Throwable ex) {
            failure.compareAndSet(null, ex);
        }
    }

    private void onConsumed() {
        CountDownLatch latch = batchLatch;
        if (latch != null) {
            latch.countDown();
        }
    }

    private void checkFailure() {
        Throwable error = failure.get();
        if (error != null) {
            throw new IllegalStateException("基准测试失败", error);
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

        private void setPublishedAtNanos(long publishedAtNanos) {
            this.publishedAtNanos = publishedAtNanos;
        }

        private void reset() {
            publishedAtNanos = 0L;
        }
    }

    private static final class NamedThreadFactory implements ThreadFactory {
        private final String prefix;
        private int idx = 1;

        private NamedThreadFactory(String prefix) {
            this.prefix = Objects.requireNonNull(prefix, "prefix");
        }

        @Override
        public synchronized Thread newThread(Runnable r) {
            Thread thread = new Thread(r, prefix + "-" + idx++);
            thread.setDaemon(true);
            return thread;
        }
    }
}
