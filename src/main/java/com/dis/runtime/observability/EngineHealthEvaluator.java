package com.dis.runtime.observability;

import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.api.HealthLevel;
import com.dis.api.StageMetricsSnapshot;
import com.dis.runtime.EngineConfig;
import com.dis.runtime.EngineState;

import java.util.ArrayList;
import java.util.List;

/**
 * 健康评估器。
 */
public final class EngineHealthEvaluator {
    public EngineHealthReport evaluate(EngineMetricsSnapshot snapshot, EngineConfig<?> config) {
        List<String> details = new ArrayList<>();
        HealthLevel level = HealthLevel.UP;

        if (snapshot.state() == EngineState.STARTED) {
            for (StageMetricsSnapshot stage : snapshot.stages()) {
                if (!stage.workerAlive()) {
                    level = HealthLevel.DOWN;
                    details.add("阶段线程已退出: " + stage.stageName());
                }
            }
        }

        if (snapshot.backlogRatio() >= config.downBacklogRatio()) {
            level = HealthLevel.DOWN;
            details.add(String.format("积压比例过高: %.2f >= %.2f", snapshot.backlogRatio(), config.downBacklogRatio()));
        } else if (snapshot.backlogRatio() >= config.degradedBacklogRatio()) {
            if (level != HealthLevel.DOWN) {
                level = HealthLevel.DEGRADED;
            }
            details.add(String.format("积压比例偏高: %.2f >= %.2f", snapshot.backlogRatio(), config.degradedBacklogRatio()));
        }

        if (snapshot.publishTranslateFailedCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("发布翻译失败数: " + snapshot.publishTranslateFailedCount());
        }

        if (snapshot.publishTimeoutCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("发布超时数: " + snapshot.publishTimeoutCount());
        }

        if (snapshot.handlerRetryCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("处理重试数: " + snapshot.handlerRetryCount());
        }

        if (snapshot.deadLetterCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("死信数: " + snapshot.deadLetterCount());
        }

        if (snapshot.gracefulShutdownTimeoutCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("优雅停机超时数: " + snapshot.gracefulShutdownTimeoutCount());
        }

        for (StageMetricsSnapshot stage : snapshot.stages()) {
            if (stage.errorCount() > 0) {
                if (level == HealthLevel.UP) {
                    level = HealthLevel.DEGRADED;
                }
                details.add("阶段错误: " + stage.stageName() + "，累计失败数: " + stage.errorCount());
            }
        }

        if (details.isEmpty()) {
            details.add("引擎运行正常，未发现异常信号");
        }

        String summary = switch (level) {
            case UP -> "状态正常";
            case DEGRADED -> "状态降级";
            case DOWN -> "状态不可用";
        };

        return new EngineHealthReport(level, summary, List.copyOf(details), snapshot);
    }
}
