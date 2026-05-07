package com.dis.api;

import java.util.List;

/**
 * 健康检查报告。
 *
 * @param level 健康等级
 * @param summary 摘要说明
 * @param details 详细信息
 * @param metrics 判定时使用的指标快照
 */
public record EngineHealthReport(
        HealthLevel level,
        String summary,
        List<String> details,
        EngineMetricsSnapshot metrics
) {
}
