package com.dis.benchmark;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class EngineVsBlockingQueueBenchmarkTest {
    private static final String[] BACKENDS = {"engine", "queue"};
    private static final int[] PRODUCER_COUNTS = {1, 2, 4};

    @Test
    void benchmarkMethodsCompleteForAllConfiguredBackendsAndProducerCounts() {
        for (String backend : BACKENDS) {
            for (int producerCount : PRODUCER_COUNTS) {
                assertDoesNotThrow(
                        () -> runBenchmarkOnce(backend, producerCount),
                        () -> "backend=" + backend + ", producerCount=" + producerCount
                );
            }
        }
    }

    private static void runBenchmarkOnce(String backend, int producerCount) throws Exception {
        EngineVsBlockingQueueBenchmark benchmark = new EngineVsBlockingQueueBenchmark();
        setField(benchmark, "backend", backend);
        setField(benchmark, "producerCount", producerCount);

        try {
            benchmark.setup();
            benchmark.publishThroughputBatch();
            benchmark.publishLatencyRoundTrip();
        } finally {
            benchmark.tearDown();
        }
    }

    private static void setField(Object target, String fieldName, Object value) throws ReflectiveOperationException {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
