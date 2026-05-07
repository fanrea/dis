package com.dis.strategy;

import com.dis.core.Sequence;

public interface WaitStrategy {
    long waitFor(long sequence, Sequence cursor, Sequence dependentSequence) throws InterruptedException;

    void signalAllWhenBlocking();
}
