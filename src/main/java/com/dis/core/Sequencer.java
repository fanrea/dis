package com.dis.core;

import java.util.concurrent.TimeUnit;

public interface Sequencer {
    Sequence cursorSequence();

    long getHighestPublishedSequence(long lowerBound, long availableSequence);

    void addGatingSequence(Sequence... sequencesToAdd);

    long next();

    long tryNext(long timeout, TimeUnit unit);

    void publish(long sequence);
}
