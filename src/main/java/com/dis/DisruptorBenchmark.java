package com.dis;

import com.dis.core.BatchEventProcessor;
import com.dis.core.MultiProducerSequencer;
import com.dis.core.RingBuffer;
import com.dis.handler.BenchmarkEventHandler;
import com.dis.handler.FatalExceptionHandler;
import com.dis.handler.LowEventHandler;
import com.dis.strategy.WaitStrategy;
import com.dis.strategy.YieldingWaitStrategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DisruptorBenchmark {
    private static final int EVENT_COUNT = 50_000;
    private static final int PRODUCER_COUNT = 4;

    public static void main(String[] args) throws Exception {
        int bufferSize = 65536;

        RingBuffer<OrderEvent> ringBuffer = new RingBuffer<>(bufferSize, OrderEvent::new);
        WaitStrategy waitStrategy = new YieldingWaitStrategy();
        MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy);

        CountDownLatch consumerLatch = new CountDownLatch(1);
        BenchmarkEventHandler handler = new BenchmarkEventHandler(consumerLatch, EVENT_COUNT);
        LowEventHandler lowHandler = new LowEventHandler();

        BatchEventProcessor<OrderEvent> high = new BatchEventProcessor<>(
                ringBuffer, sequencer, null, waitStrategy, handler, new FatalExceptionHandler<>()
        );
        BatchEventProcessor<OrderEvent> low = new BatchEventProcessor<>(
                ringBuffer, sequencer, high.getSequence(), waitStrategy, lowHandler, new FatalExceptionHandler<>()
        );

        sequencer.addGatingSequence(high.getSequence());
        sequencer.addGatingSequence(low.getSequence());

        new Thread(high, "Benchmark-Consumer").start();
        new Thread(low, "Low-Consumer").start();

        ExecutorService executor = Executors.newFixedThreadPool(PRODUCER_COUNT);
        int countPerProducer = EVENT_COUNT / PRODUCER_COUNT;

        System.out.println("========== benchmark start ==========");
        System.out.println("event count: " + EVENT_COUNT);
        System.out.println("producer threads: " + PRODUCER_COUNT);
        System.out.println("wait strategy: YieldingWaitStrategy");
        System.out.println("ring buffer size: " + bufferSize);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < PRODUCER_COUNT; i++) {
            executor.submit(() -> {
                for (int j = 0; j < countPerProducer; j++) {
                    long seq = sequencer.next();
                    ringBuffer.get(seq).setValues("TX-ORDER", 199);
                    sequencer.publish(seq);
                }
            });
        }

        consumerLatch.await();

        long endTime = System.currentTimeMillis();
        long costTimeMs = endTime - startTime;
        long tps = (EVENT_COUNT * 1000L) / Math.max(1, costTimeMs);

        System.out.println("========== benchmark report ==========");
        System.out.println("cost(ms): " + costTimeMs);
        System.out.println("throughput(ops/sec): " + tps);

        high.halt();
        low.halt();
        executor.shutdown();
    }
}
