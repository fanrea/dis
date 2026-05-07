package com.dis.runtime.observability;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 单阶段运行时指标。
 */
public final class StageMetrics {
    private final String stageName;
    private final LongAdder consumed = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LatencyWindow latencyWindow = new LatencyWindow(4096);
    private final AtomicLong lastSequence = new AtomicLong(-1L);

    public StageMetrics(String stageName) {
        this.stageName = stageName;
    }

    public String stageName() {
        return stageName;
    }

    public void recordSuccess(long sequence, long latencyNanos) {
        consumed.increment();
        latencyWindow.recordNanos(latencyNanos);
        lastSequence.set(sequence);
    }

    public void recordError(long sequence, long latencyNanos) {
        errors.increment();
        latencyWindow.recordNanos(latencyNanos);
        lastSequence.set(sequence);
    }

    public long consumedCount() {
        return consumed.sum();
    }

    public long errorCount() {
        return errors.sum();
    }

    public long lastSequence() {
        return lastSequence.get();
    }

    public LatencyWindow.Stats latencyStats() {
        return latencyWindow.snapshot();
    }
}
