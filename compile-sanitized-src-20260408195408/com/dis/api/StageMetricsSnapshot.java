package com.dis.api;

/**
 * 阶段指标快照。
 *
 * @param stageName 阶段名称
 * @param consumedCount 已消费总数
 * @param errorCount 错误总数
 * @param avgLatencyMicros 平均耗时（微秒）
 * @param p95LatencyMicros P95 耗时（微秒）
 * @param p99LatencyMicros P99 耗时（微秒）
 * @param lastSequence 当前阶段最新序号
 * @param lag 相对生产游标的积压
 * @param workerAlive 工作线程是否存活
 */
public record StageMetricsSnapshot(
        String stageName,
        long consumedCount,
        long errorCount,
        double avgLatencyMicros,
        double p95LatencyMicros,
        double p99LatencyMicros,
        long lastSequence,
        long lag,
        boolean workerAlive
) {
}
