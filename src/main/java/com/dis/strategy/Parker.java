package com.dis.strategy;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

// LockSupport park/unpark 的轻量封装。
// 核心逻辑：
// 1. park 前先把当前线程注册到 waiters，signal 时逐个 unpark。
// 2. signalEpoch 用来识别“检查条件之后、真正 park 之前”发生的 signal，避免丢唤醒。
// 3. shouldPark 由调用方提供，park 前二次检查实际等待条件是否仍然成立。
final class Parker {
    private final Set<Thread> waiters = ConcurrentHashMap.newKeySet(); // 当前可能处于 park 的等待线程。
    private final AtomicLong signalEpoch = new AtomicLong(); // 每次 signal 递增，用于识别并发唤醒。

    void parkNanos(long nanos, BooleanSupplier shouldPark) {
        if (nanos <= 0L) {
            return;
        }

        Thread current = Thread.currentThread();
        long observedEpoch = signalEpoch.get();
        waiters.add(current);
        try {
            // 注册后再检查条件和 signal 版本，避免刚发布唤醒就进入长时间 park。
            if (!shouldPark.getAsBoolean() || signalEpoch.get() != observedEpoch) {
                return;
            }
            LockSupport.parkNanos(this, nanos);
        } finally {
            waiters.remove(current);
        }
    }

    void signalAll() {
        signalEpoch.incrementAndGet();
        for (Thread waiter : waiters) {
            LockSupport.unpark(waiter);
        }
    }
}
