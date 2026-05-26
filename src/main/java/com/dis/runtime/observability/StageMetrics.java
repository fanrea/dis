package com.dis.runtime.observability;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

// 单阶段运行时指标。
public final class StageMetrics {
    private final String stageName;
    private final LongAdder successCount = new LongAdder();
    private final LongAdder errorCount = new LongAdder();
    private final LongAdder retryCount = new LongAdder();
    private final LongAdder deadLetterCount = new LongAdder();
    private final LongAdder skippedCount = new LongAdder();
    private final LatencyWindow latencyWindow = new LatencyWindow(4096);
    private final AtomicLong lastSequence = new AtomicLong(-1L);
    private volatile String lastErrorMessageText;

    public StageMetrics(String stageName) {
        this.stageName = stageName;
    }

    public String stageName() {
        return stageName;
    }

    public void recordSuccess(long sequence, long latencyNanos) {
        successCount.increment();
        latencyWindow.recordNanos(latencyNanos);
        lastSequence.set(sequence);
    }

    public void recordRetry(long sequence, long latencyNanos, Throwable cause) {
        // retry 也计入延迟窗口，可反映“最终成功前的真实处理成本”。
        retryCount.increment();
        latencyWindow.recordNanos(latencyNanos);
        lastSequence.set(sequence);
        lastErrorMessageText = messageOf(cause);
    }

    public void recordError(long sequence, long latencyNanos, Throwable cause) {
        errorCount.increment();
        latencyWindow.recordNanos(latencyNanos);
        lastSequence.set(sequence);
        lastErrorMessageText = messageOf(cause);
    }

    public void recordDeadLetter(long sequence, long latencyNanos, Throwable cause) {
        deadLetterCount.increment();
        latencyWindow.recordNanos(latencyNanos);
        lastSequence.set(sequence);
        lastErrorMessageText = messageOf(cause);
    }

    public void recordSkippedPublishFailure(long sequence, Throwable cause) {
        // 发布阶段失败导致的“有序跳过”，不算业务处理错误。
        skippedCount.increment();
        lastSequence.set(sequence);
        lastErrorMessageText = messageOf(cause);
    }

    public long successCount() {
        return successCount.sum();
    }

    public long errorCount() {
        return errorCount.sum();
    }

    public long retryCount() {
        return retryCount.sum();
    }

    public long deadLetterCount() {
        return deadLetterCount.sum();
    }

    public long skippedCount() {
        return skippedCount.sum();
    }

    public long consumedCount() {
        return successCount();
    }

    public long lastSequence() {
        return lastSequence.get();
    }

    public String lastErrorMessage() {
        return lastErrorMessageText;
    }

    public LatencyWindow.Stats latencyStats() {
        return latencyWindow.snapshot();
    }

    private static String messageOf(Throwable cause) {
        return cause == null ? null : cause.getMessage();
    }
}
