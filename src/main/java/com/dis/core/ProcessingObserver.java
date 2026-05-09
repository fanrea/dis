package com.dis.core;

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
