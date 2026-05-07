package com.dis.core;

import com.dis.strategy.WaitStrategy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

public final class MultiProducerSequencer implements Sequencer {
    private final int bufferSize;
    private final WaitStrategy waitStrategy;
    private final Sequence cursor = new Sequence(-1);
    private volatile Sequence[] gatingSequences;

    private final int[] availableBuffer;
    private final int indexMask;
    private final int indexShift;

    private static final VarHandle AVAILABLE_VH = MethodHandles.arrayElementVarHandle(int[].class);

    public MultiProducerSequencer(int bufferSize, WaitStrategy waitStrategy, Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
        this.gatingSequences = gatingSequences == null ? new Sequence[0] : Arrays.copyOf(gatingSequences, gatingSequences.length);
        this.availableBuffer = new int[bufferSize];
        this.indexMask = bufferSize - 1;
        this.indexShift = Integer.numberOfTrailingZeros(bufferSize);

        for (int i = 0; i < bufferSize; i++) {
            AVAILABLE_VH.set(availableBuffer, i, -1);
        }
    }

    @Override
    public long next() {
        long current;
        long next;
        long wrapPoint;
        long cachedGatingSequence = cursor.getVolatile() - bufferSize;

        do {
            current = cursor.getVolatile();
            next = current + 1;
            wrapPoint = next - bufferSize;

            if (wrapPoint > cachedGatingSequence) {
                long minSequence = getMinimumSequence(gatingSequences, current);
                if (wrapPoint > minSequence) {
                    LockSupport.parkNanos(1L);
                    continue;
                }
                cachedGatingSequence = minSequence;
            }
        } while (!cursor.compareAndSet(current, next));

        return next;
    }

    @Override
    public void publish(long sequence) {
        int index = (int) (sequence & indexMask);
        int flag = (int) (sequence >>> indexShift);
        AVAILABLE_VH.setRelease(availableBuffer, index, flag);
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public Sequence cursorSequence() {
        return cursor;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++) {
            int index = (int) (sequence & indexMask);
            int flag = (int) (sequence >>> indexShift);
            if ((int) AVAILABLE_VH.getAcquire(availableBuffer, index) != flag) {
                return sequence - 1;
            }
        }
        return availableSequence;
    }

    private static long getMinimumSequence(Sequence[] sequences, long defaultValue) {
        long min = Long.MAX_VALUE;
        for (Sequence s : sequences) {
            long v = s.getVolatile();
            min = Math.min(min, v);
        }
        return min == Long.MAX_VALUE ? defaultValue : min;
    }

    @Override
    public synchronized void addGatingSequence(Sequence... sequencesToAdd) {
        if (sequencesToAdd == null || sequencesToAdd.length == 0) {
            return;
        }
        Sequence[] currentSequences = this.gatingSequences;
        Sequence[] updatedSequences = Arrays.copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);
        System.arraycopy(sequencesToAdd, 0, updatedSequences, currentSequences.length, sequencesToAdd.length);
        this.gatingSequences = updatedSequences;
    }
}
