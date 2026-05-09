package com.dis.api;

public record DeadLetterEvent<E>(
        String stageName,
        long sequence,
        E eventSnapshotOrReference,
        Throwable cause,
        int attempts,
        long deadLetterAtMillis
) {
}
