package com.dis.runtime;

// 引擎生命周期状态。
// NEW: 只允许配置 pipeline。
// STARTED: 处理线程已经启动，允许发布事件。
// DRAINING: 正在优雅停机，不再接收新事件，只等待已发布事件被消费完成。
// SHUTDOWN: 已关闭，不能再次启动或发布。
public enum EngineState {
    NEW("新建"), // 只允许配置 pipeline。
    STARTED("运行中"), // 处理线程已经启动，允许发布事件。
    DRAINING("排空中"), // 正在优雅停机，不再接收新事件，只等待已发布事件被消费完成。
    SHUTDOWN("已关闭"); // 已关闭，不能再次启动或发布。

    private final String text;

    EngineState(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return text;
    }
}
