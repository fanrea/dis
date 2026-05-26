package com.dis.strategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// 限频唤醒策略。
// 至少间隔 minIntervalNanos 才允许下一次唤醒。
public final class RateLimitedSignalPolicy implements PublishSignalPolicy {
    private final long minIntervalNanos;
    private final AtomicLong lastSignalNanos = new AtomicLong(Long.MIN_VALUE);

    public RateLimitedSignalPolicy(long interval, TimeUnit unit) {
        if (interval < 0L) {
            throw new IllegalArgumentException("interval 必须大于等于 0");
        }
        if (unit == null) {
            throw new IllegalArgumentException("unit 不能为空");
        }
        this.minIntervalNanos = unit.toNanos(interval);
    }

    @Override
    public boolean shouldSignal(long sequence) {
        if (minIntervalNanos == 0L) {
            return true;
        }
        long now = System.nanoTime();
        while (true) {
            long prev = lastSignalNanos.get();
            if (now - prev < minIntervalNanos) {
                return false;
            }
            if (lastSignalNanos.compareAndSet(prev, now)) {
                return true;
            }
        }
    }
}
