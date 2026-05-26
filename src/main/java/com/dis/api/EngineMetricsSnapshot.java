package com.dis.api;

import com.dis.runtime.EngineState;

import java.util.List;

// 引擎指标总览快照。
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
