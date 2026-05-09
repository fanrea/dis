package com.dis.api;

import com.dis.runtime.EngineState;

import java.util.List;

/**
 * 引擎指标总览快照。
 *
 * @param state 引擎状态
 * @param uptimeMillis 运行时长（毫秒）
 * @param publishedCount 发布成功数
 * @param publishErrorCount 发布失败总数
 * @param publishTranslateFailedCount 翻译失败数
 * @param publishTimeoutCount 发布超时数
 * @param consumerSkippedTranslateFailedCount 消费端跳过翻译失败数
 * @param handlerRetryCount 处理重试数
 * @param deadLetterCount 死信数
 * @param gracefulShutdownCount 优雅停机成功次数
 * @param gracefulShutdownTimeoutCount 优雅停机超时次数
 * @param publishAvgLatencyMicros 发布平均耗时（微秒）
 * @param publishP95LatencyMicros 发布 P95 耗时（微秒）
 * @param publishP99LatencyMicros 发布 P99 耗时（微秒）
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
        long publishTranslateFailedCount,
        long publishTimeoutCount,
        long consumerSkippedTranslateFailedCount,
        long handlerRetryCount,
        long deadLetterCount,
        long gracefulShutdownCount,
        long gracefulShutdownTimeoutCount,
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
