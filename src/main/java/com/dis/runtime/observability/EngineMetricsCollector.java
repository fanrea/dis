package com.dis.runtime.observability;

import com.dis.api.EngineMetricsSnapshot;
import com.dis.api.StageMetricsSnapshot;
import com.dis.runtime.EngineState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 引擎级指标采集器。
 */
public final class EngineMetricsCollector {
    private final LongAdder published = new LongAdder();
    private final LongAdder publishErrors = new LongAdder();
    private final LatencyWindow publishLatency = new LatencyWindow(4096);
    private final ConcurrentMap<String, StageMetrics> stageMetricsMap = new ConcurrentHashMap<>();

    public StageMetrics registerStage(String stageName) {
        return stageMetricsMap.computeIfAbsent(stageName, StageMetrics::new);
    }

    public void recordPublishSuccess(long latencyNanos) {
        published.increment();
        publishLatency.recordNanos(latencyNanos);
    }

    public void recordPublishError(long latencyNanos) {
        publishErrors.increment();
        publishLatency.recordNanos(latencyNanos);
    }

    public EngineMetricsSnapshot snapshot(EngineState state,
                                          long uptimeMillis,
                                          long cursor,
                                          long minConsumerSequence,
                                          int bufferSize,
                                          List<StageRuntimeView> runtimeViews) {
        long lag = Math.max(0, cursor - minConsumerSequence);
        double backlogRatio = bufferSize <= 0 ? 0.0 : Math.min(1.0, lag * 1.0 / bufferSize);

        LatencyWindow.Stats publishStats = publishLatency.snapshot();
        List<StageMetricsSnapshot> stages = new ArrayList<>(runtimeViews.size());
        for (StageRuntimeView view : runtimeViews) {
            StageMetrics stage = stageMetricsMap.computeIfAbsent(view.stageName(), StageMetrics::new);
            LatencyWindow.Stats stats = stage.latencyStats();
            long stageLag = Math.max(0, cursor - view.sequence());
            stages.add(new StageMetricsSnapshot(
                    view.stageName(),
                    stage.consumedCount(),
                    stage.errorCount(),
                    nanosToMicros(stats.avgNanos()),
                    nanosToMicros(stats.p95Nanos()),
                    nanosToMicros(stats.p99Nanos()),
                    view.sequence(),
                    stageLag,
                    view.alive()
            ));
        }

        return new EngineMetricsSnapshot(
                state,
                uptimeMillis,
                published.sum(),
                publishErrors.sum(),
                nanosToMicros(publishStats.avgNanos()),
                nanosToMicros(publishStats.p95Nanos()),
                nanosToMicros(publishStats.p99Nanos()),
                cursor,
                minConsumerSequence,
                lag,
                backlogRatio,
                stages
        );
    }

    private static double nanosToMicros(double nanos) {
        return nanos / 1_000.0;
    }

    public record StageRuntimeView(String stageName, long sequence, boolean alive) {
    }
}
