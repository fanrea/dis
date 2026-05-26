package com.dis.runtime.observability;

import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;

import java.util.StringJoiner;
import java.util.logging.Logger;

// 周期观测日志输出器。
public final class ObservabilityLogger {
    private static final Logger LOGGER = Logger.getLogger(ObservabilityLogger.class.getName());

    public void logPeriodic(EngineHealthReport report) {
        EngineMetricsSnapshot m = report.metrics();
        StringJoiner joiner = new StringJoiner(" | ");
        joiner.add("引擎观测");
        joiner.add("健康=" + report.level());
        joiner.add("状态=" + m.state());
        joiner.add("运行时长毫秒=" + m.uptimeMillis());
        joiner.add("发布成功=" + m.publishedCount());
        joiner.add("发布失败=" + m.publishErrorCount());
        joiner.add("翻译失败=" + m.publishTranslateFailedCount());
        joiner.add("发布超时=" + m.publishTimeoutCount());
        joiner.add("消费跳过=" + m.consumerSkippedTranslateFailedCount());
        joiner.add("处理重试=" + m.handlerRetryCount());
        joiner.add("死信数=" + m.deadLetterCount());
        joiner.add("优雅停机成功=" + m.gracefulShutdownCount());
        joiner.add("优雅停机超时=" + m.gracefulShutdownTimeoutCount());
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
                    "阶段观测 | 名称=%s | 存活=%s | 成功=%d | 错误=%d | 重试=%d | 死信=%d | 跳过=%d | 最近序号=%d | 积压=%d | 平均微秒=%.2f | P95微秒=%.2f | P99微秒=%.2f | 最近错误=%s",
                    stage.stageName(),
                    stage.workerAlive(),
                    stage.successCount(),
                    stage.errorCount(),
                    stage.retryCount(),
                    stage.deadLetterCount(),
                    stage.skippedCount(),
                    stage.lastSequence(),
                    stage.lag(),
                    stage.avgLatencyMicros(),
                    stage.p95LatencyMicros(),
                    stage.p99LatencyMicros(),
                    stage.lastErrorMessage()
            );
            LOGGER.info(line);
        }

        for (String detail : report.details()) {
            LOGGER.info("健康明细 | " + detail);
        }
    }
}
