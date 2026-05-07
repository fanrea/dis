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
 *
 * 判定规则：
 * 1. 运行中线程死亡 -> DOWN。
 * 2. 积压比例超过阈值 -> DEGRADED 或 DOWN。
 * 3. 发布或阶段出现错误 -> 至少 DEGRADED。
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

        if (snapshot.publishErrorCount() > 0) {
            if (level == HealthLevel.UP) {
                level = HealthLevel.DEGRADED;
            }
            details.add("发布路径出现异常，累计失败数: " + snapshot.publishErrorCount());
        }

        for (StageMetricsSnapshot stage : snapshot.stages()) {
            if (stage.errorCount() > 0) {
                if (level == HealthLevel.UP) {
                    level = HealthLevel.DEGRADED;
                }
                details.add("阶段出现异常: " + stage.stageName() + "，累计失败数: " + stage.errorCount());
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
