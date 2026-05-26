package com.dis.core;

import java.util.function.Supplier;

// 固定大小的环形数组，是事件存储层最核心的数据结构。
// 核心逻辑：
// 1. 构造时一次性预分配所有槽位，运行期间循环复用，减少 GC 压力。
// 2. 通过 sequence & indexMask 把无限增长的逻辑序号映射到有限数组下标。
// 3. bufferSize 必须是 2 的幂，这样取模可以退化成位运算。
public class RingBuffer<E> {
    private final Object[] entries; // 真实存储数组；泛型数组不能直接创建，所以这里用 Object[] 承载。
    private final int bufferSize; // RingBuffer 容量。
    private final long indexMask; // bufferSize - 1，用于与运算把 sequence 映射为数组下标。

    public RingBuffer(int bufferSize, Supplier<E> factory) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("缓冲区大小必须是 2 的幂");
        }
        this.bufferSize = bufferSize;
        entries = new Object[bufferSize];
        indexMask = bufferSize - 1;
        for (int i = 0; i < bufferSize; i++) {
            // 每个槽位提前放入一个对象，后续发布时只改对象内容，不再创建新事件。
            entries[i] = factory.get();
        }
    }

    @SuppressWarnings("unchecked")
    public E get(long sequence) {
        // 等价于 sequence % bufferSize，但位运算在热点路径更轻量。
        int idx = (int) (sequence & indexMask);
        return (E) entries[idx];
    }

    public int size() {
        return bufferSize;
    }

}
