package com.dis.api;

/**
 * 健康等级：
 * UP 表示系统状态正常；
 * DEGRADED 表示可用但已降级；
 * DOWN 表示不可用。
 */
public enum HealthLevel {
    UP,
    DEGRADED,
    DOWN
}
