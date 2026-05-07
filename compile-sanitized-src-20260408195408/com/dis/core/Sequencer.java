package com.dis.core;

public interface Sequencer {
    Sequence cursorSequence();

    long getHighestPublishedSequence(long lowerBound, long availableSequence);

    void addGatingSequence(Sequence... sequencesToAdd);

    long next();

    void publish(long sequence);
}
