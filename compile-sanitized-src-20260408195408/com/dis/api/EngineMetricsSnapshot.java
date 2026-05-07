package com.dis.api;

import com.dis.runtime.EngineState;

import java.util.List;

/**
 * 引擎指标总览快照。
 *
 * @param state 引擎状态
 * @param uptimeMillis 运行时长（毫秒）
 * @param publishedCount 发布成功总数
 * @param publishErrorCount 发布失败总数
 * @param publishAvgLatencyMicros 发布平均耗时（微秒）
 * @param publishP95LatencyMicros 发布 P95（微秒）
 * @param publishP99LatencyMicros 发布 P99（微秒）
 * @param cursor 生产游标
 * @param minConsumerSequence 最慢消费者序号
 * @param globalLag 全局积压
 * @param backlogRatio 积压比例
 * @param stages 各阶段快照
 */
public record EngineMetricsSnapshot(
        EngineState state,
        long uptimeMillis,
        long publishedCount,
        long publishErrorCount,
        double publishAvgLatencyMicros,
        double publishP95LatencyMicros,
        double publishP99LatencyMicros,
        long cursor,
        long minConsumerSequence,
        long globalLag,
        double backlogRatio,
        List<StageMetricsSnapshot> stages
) {
}
