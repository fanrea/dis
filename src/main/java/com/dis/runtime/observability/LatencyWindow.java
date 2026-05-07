package com.dis.runtime.observability;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 固定窗口延迟采样器。
 *
 * 目标：
 * 1. 写入无锁，适合高频路径。
 * 2. 快照时计算近似分位数。
 * 3. 固定内存占用。
 */
public final class LatencyWindow {
    private final AtomicLongArray values;
    private final int capacity;
    private final AtomicLong index = new AtomicLong(0);

    public LatencyWindow(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        this.capacity = capacity;
        this.values = new AtomicLongArray(capacity);
    }

    public void recordNanos(long nanos) {
        if (nanos < 0) {
            return;
        }
        long pos = index.getAndIncrement();
        int slot = (int) (pos % capacity);
        values.set(slot, nanos);
    }

    public Stats snapshot() {
        long written = index.get();
        int size = (int) Math.min(written, capacity);
        if (size == 0) {
            return new Stats(0.0, 0.0, 0.0);
        }

        long[] copy = new long[size];
        for (int i = 0; i < size; i++) {
            copy[i] = values.get(i);
        }
        Arrays.sort(copy);

        double avg = Arrays.stream(copy).average().orElse(0.0);
        long p95 = copy[(int) Math.min(size - 1, Math.ceil(size * 0.95) - 1)];
        long p99 = copy[(int) Math.min(size - 1, Math.ceil(size * 0.99) - 1)];
        return new Stats(avg, p95, p99);
    }

    public record Stats(double avgNanos, double p95Nanos, double p99Nanos) {
    }
}
