package com.dis.runtime.observability;

import com.dis.api.EngineHealthReport;
import com.dis.api.EngineMetricsSnapshot;
import com.dis.api.HealthLevel;
import com.dis.api.StageMetricsSnapshot;
import com.dis.runtime.EngineConfig;
import com.dis.runtime.EngineState;

import java.util.ArrayList;
import java.util.List;

// 健康评估器。
// 评估优先级：
// 1. 不可用信号（线程死亡、严重积压）优先判定 DOWN。
// 2. 其次是降级信号（翻译失败、超时、重试、死信、一般错误）。
// 3. 无异常信号判定 UP。
public final class EngineHealthEvaluator {
    public EngineHealthReport evaluate(EngineMetricsSnapshot snapshot, EngineConfig<?> config) {
        List<String> details = new ArrayList<>();
        HealthLevel level = HealthLevel.UP;

        if (snapshot.state() == EngineState.STARTED) {
            for (StageMetricsSnapshot stage : snapshot.stages()) {
                if (!stage.workerAlive()) {
                    level = HealthLevel.DOWN;
                    details.add("阶段工作线程已退出：" + stage.stageName());
                }
            }
        }

        if (snapshot.backlogRatio() >= config.downBacklogRatio()) {
            level = HealthLevel.DOWN;
            details.add(String.format("积压过高：%.2f >= %.2f", snapshot.backlogRatio(), config.downBacklogRatio()));
        } else if (snapshot.backlogRatio() >= config.degradedBacklogRatio()) {
            if (level != HealthLevel.DOWN) {
                level = HealthLevel.DEGRADED;
            }
            details.add(String.format("积压升高：%.2f >= %.2f", snapshot.backlogRatio(), config.degradedBacklogRatio()));
        }

        if (snapshot.publishTranslateFailedCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("发布转换失败次数=" + snapshot.publishTranslateFailedCount());
        }

        if (snapshot.publishTimeoutCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("发布超时次数=" + snapshot.publishTimeoutCount());
        }

        if (snapshot.handlerRetryCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("处理器重试次数=" + snapshot.handlerRetryCount());
        }

        if (snapshot.deadLetterCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("死信次数=" + snapshot.deadLetterCount());
        }

        if (snapshot.gracefulShutdownTimeoutCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("优雅停机超时次数=" + snapshot.gracefulShutdownTimeoutCount());
        }

        for (StageMetricsSnapshot stage : snapshot.stages()) {
            if (stage.errorCount() > 0) {
                if (level == HealthLevel.UP) {
                    level = HealthLevel.DEGRADED;
                }
                details.add("阶段错误：" + stage.stageName() + "，次数=" + stage.errorCount());
            }
        }

        if (details.isEmpty()) {
            details.add("引擎健康");
        }

        String summary = switch (level) {
            case UP -> "正常";
            case DEGRADED -> "降级";
            case DOWN -> "不可用";
        };

        return new EngineHealthReport(level, summary, List.copyOf(details), snapshot);
    }
}
