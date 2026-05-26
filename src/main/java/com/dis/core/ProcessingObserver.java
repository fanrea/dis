package com.dis.core;

// Stage 处理过程观测钩子。
// core 包只负责在关键处理点发出回调，runtime 层负责把这些回调转换为指标。
public interface ProcessingObserver<E> {
    default void onSuccess(long sequence, E event, long latencyNanos) {
    }

    default void onFailure(long sequence, E event, Throwable cause, long latencyNanos) {
    }

    default void onRetry(long sequence, E event, Throwable cause, int attempt, long latencyNanos) {
    }

    default void onDeadLetter(long sequence, E event, Throwable cause, int attempts, long latencyNanos) {
    }

    default void onSkippedPublishFailure(long sequence, E event, Throwable cause) {
    }
}
