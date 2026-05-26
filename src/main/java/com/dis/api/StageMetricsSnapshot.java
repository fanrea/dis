package com.dis.api;

// 单阶段指标快照。
public record StageMetricsSnapshot(
        String stageName,
        long successCount,
        long errorCount,
        long retryCount,
        long deadLetterCount,
        long skippedCount,
        double avgLatencyMicros,
        double p95LatencyMicros,
        double p99LatencyMicros,
        long lastSequence,
        long lag,
        boolean workerAlive,
        String lastErrorMessage
) {
    // consumedCount = 成功处理 + 跳过发布失败。
    public long consumedCount() {
        return successCount + skippedCount;
    }
}
