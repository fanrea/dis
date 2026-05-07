package com.dis.core;

import java.util.function.Supplier;

public class RingBuffer<E> {
    private final Object[] entries;
    private final int bufferSize;
    private final long indexMask;

    public RingBuffer(int bufferSize, Supplier<E> factory) {
        if(Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("buffer size must be a power of 2");
        }
        this.bufferSize = bufferSize;
        entries = new Object[bufferSize];
        indexMask = bufferSize - 1;
        for(int i = 0; i < bufferSize; i++) {
            entries[i] = factory.get();
        }
    }
    public E get(long sequence) {
        int idx = (int) (sequence & indexMask);
        return (E) entries[idx];
    }

    public int size() {
        return bufferSize;
    }

}
