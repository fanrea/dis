package com.dis.runtime.observability;

import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;

import java.util.StringJoiner;
import java.util.logging.Logger;

/**
 * 标准观测日志输出器。
 */
public final class ObservabilityLogger {
    private static final Logger LOGGER = Logger.getLogger(ObservabilityLogger.class.getName());

    public void logPeriodic(EngineHealthReport report) {
        EngineMetricsSnapshot m = report.metrics();
        StringJoiner joiner = new StringJoiner(" | ");
        joiner.add("引擎观测日志");
        joiner.add("健康等级=" + report.level());
        joiner.add("状态=" + m.state());
        joiner.add("运行时长毫秒=" + m.uptimeMillis());
        joiner.add("发布成功=" + m.publishedCount());
        joiner.add("发布失败=" + m.publishErrorCount());
        joiner.add(String.format("发布平均延迟微秒=%.2f", m.publishAvgLatencyMicros()));
        joiner.add(String.format("发布P95微秒=%.2f", m.publishP95LatencyMicros()));
        joiner.add(String.format("发布P99微秒=%.2f", m.publishP99LatencyMicros()));
        joiner.add("游标=" + m.cursor());
        joiner.add("最慢消费序号=" + m.minConsumerSequence());
        joiner.add("全局积压=" + m.globalLag());
        joiner.add(String.format("积压比例=%.4f", m.backlogRatio()));

        LOGGER.info(joiner.toString());

        for (var stage : m.stages()) {
            String line = String.format(
                    "阶段观测 | 名称=%s | 线程存活=%s | 已消费=%d | 错误=%d | 当前序号=%d | 阶段积压=%d | 平均微秒=%.2f | P95微秒=%.2f | P99微秒=%.2f",
                    stage.stageName(),
                    stage.workerAlive(),
                    stage.consumedCount(),
                    stage.errorCount(),
                    stage.lastSequence(),
                    stage.lag(),
                    stage.avgLatencyMicros(),
                    stage.p95LatencyMicros(),
                    stage.p99LatencyMicros()
            );
            LOGGER.info(line);
        }

        for (String detail : report.details()) {
            LOGGER.info("健康明细 | " + detail);
        }
    }
}
