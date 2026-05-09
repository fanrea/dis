package com.dis.api;

/**
 * 阶段指标快照。
 *
 * @param stageName 阶段名称
 * @param successCount 成功数
 * @param errorCount 错误数
 * @param retryCount 重试数
 * @param deadLetterCount 死信数
 * @param skippedCount 跳过数
 * @param avgLatencyMicros 平均耗时（微秒）
 * @param p95LatencyMicros P95 耗时（微秒）
 * @param p99LatencyMicros P99 耗时（微秒）
 * @param lastSequence 最后处理序号
 * @param lag 相对生产游标的积压
 * @param workerAlive 工作线程是否存活
 * @param lastErrorMessage 最近错误信息
 */
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
    public long consumedCount() {
        return successCount + skippedCount;
    }
}
