package com.dis.api;

// 健康等级。
// UP: 正常可用。
// DEGRADED: 可用但已降级。
// DOWN: 不可用。
public enum HealthLevel {
    UP("正常"),
    DEGRADED("降级"),
    DOWN("不可用");

    private final String text;

    HealthLevel(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
