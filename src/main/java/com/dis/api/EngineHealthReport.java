package com.dis.api;

import java.util.List;

// 健康检查报告。
public record EngineHealthReport(
        HealthLevel level,
        String summary,
        List<String> details,
        EngineMetricsSnapshot metrics
) {
}
