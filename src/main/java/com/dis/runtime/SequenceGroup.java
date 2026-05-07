package com.dis.runtime;

import com.dis.core.Sequence;

/**
 * Sequence 组合视图。
 *
 * 用于在多上游依赖场景下获取“最慢进度”。
 */
final class SequenceGroup extends Sequence {
    private final Sequence[] sequences;

    SequenceGroup(Sequence... sequences) {
        super(-1);
        this.sequences = sequences;
    }

    @Override
    public long getVolatile() {
        long min = Long.MAX_VALUE;
        for (Sequence sequence : sequences) {
            long value = sequence.getVolatile();
            if (value < min) {
                min = value;
            }
        }
        return min == Long.MAX_VALUE ? -1 : min;
    }

    @Override
    public long getAcquire() {
        return getVolatile();
    }

    @Override
    public void setRelease(long value) {
        throw new UnsupportedOperationException("SequenceGroup is read-only");
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        throw new UnsupportedOperationException("SequenceGroup is read-only");
    }
}
