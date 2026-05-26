package com.dis.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

// 轻量级序列号容器，是生产者、消费者和等待策略之间交换进度的基础协议。
// 核心逻辑：
// 1. 用 VarHandle 明确控制 acquire/release 语义，避免不必要的全量 volatile 栅栏。
// 2. 前后填充字段用于减轻伪共享，降低多个热 Sequence 落在同一缓存行时的抖动。
// 3. CAS 用于生产者抢 claim、worker 抢任务；release 写用于发布消费者处理进度。
public class Sequence {

    @SuppressWarnings("unused")
    private long p1, p2, p3, p4, p5, p6, p7; // 前置填充，尽量把 value 独占在缓存行里。

    private volatile long value; // 当前序号值。

    @SuppressWarnings("unused")
    private long q1, q2, q3, q4, q5, q6, q7; // 后置填充，和前置填充一起减少伪共享。

    private static final VarHandle vh; // value 字段的 VarHandle，用于精确控制内存语义。
    static {
        try {
            vh = MethodHandles.lookup().findVarHandle(Sequence.class, "value", long.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public Sequence(long value) {
        // -1 常用作初始进度，表示还没有处理任何 sequence。
        this.value = value;
    }

    public long getAcquire() {
        // acquire 读取与对端 release 写入配对，用于读取“已发布的进度及其之前的写入”。
        return (long) vh.getAcquire(this);
    }

    public void setRelease(long value) {
        // release 写入用于发布当前处理进度，保证该进度之前的处理结果不会被重排到写入之后。
        vh.setRelease(this, value);
    }

    public boolean compareAndSet(long expect, long update) {
        // CAS 用在生产者 claim 和 worker 抢任务路径上，保证同一个 sequence 只被一个线程成功占用。
        return vh.compareAndSet(this, expect, update);
    }

    public long getVolatile() {
        // 普通 volatile 读取用于需要最强可见性的进度扫描。
        return value;
    }
}
