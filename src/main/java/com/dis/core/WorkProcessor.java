package com.dis.core;

import com.dis.api.DeadLetterEvent;
import com.dis.api.DeadLetterHandler;
import com.dis.api.RetryPolicy;
import com.dis.handler.ExceptionHandler;
import com.dis.handler.WorkHandler;
import com.dis.strategy.WaitStrategy;

/**
 * worker pool 处理器。
 *
 * 每个事件只会被一个 worker 抢占处理。
 */
public final class WorkProcessor<E> implements Runnable {
    private final RingBuffer<?> ringBuffer;
    private final Sequencer sequencer;
    private final WaitStrategy waitStrategy;
    private final ExceptionHandler<E> exceptionHandler;
    private final WorkHandler<E> workHandler;
    private final Sequence workSequence;
    private final Sequence dependentSequence;
    private final RetryPolicy retryPolicy;
    private final DeadLetterHandler<E> deadLetterHandler;
    private final ProcessingObserver<E> observer;
    private final String stageName;
    private final Sequence sequence = new Sequence(-1);

    private volatile boolean running = true;

    public WorkProcessor(RingBuffer<?> ringBuffer,
                         Sequencer sequencer,
                         Sequence workSequence,
                         Sequence dependentSequence,
                         WaitStrategy waitStrategy,
                         WorkHandler<E> workHandler,
                         ExceptionHandler<E> exceptionHandler) {
        this(ringBuffer, sequencer, workSequence, dependentSequence, waitStrategy, workHandler, exceptionHandler,
                RetryPolicy.noRetry(), event -> {
                }, "stage", new ProcessingObserver<>() {
        });
    }

    public WorkProcessor(RingBuffer<?> ringBuffer,
                         Sequencer sequencer,
                         Sequence workSequence,
                         Sequence dependentSequence,
                         WaitStrategy waitStrategy,
                         WorkHandler<E> workHandler,
                         ExceptionHandler<E> exceptionHandler,
                         RetryPolicy retryPolicy,
                         DeadLetterHandler<E> deadLetterHandler,
                         String stageName,
                         ProcessingObserver<E> observer) {
        this.ringBuffer = ringBuffer;
        this.sequencer = sequencer;
        this.workSequence = workSequence;
        this.dependentSequence = dependentSequence == null ? sequencer.cursorSequence() : dependentSequence;
        this.waitStrategy = waitStrategy;
        this.workHandler = workHandler;
        this.exceptionHandler = exceptionHandler;
        this.retryPolicy = retryPolicy;
        this.deadLetterHandler = deadLetterHandler;
        this.stageName = stageName == null || stageName.isBlank() ? "stage" : stageName;
        this.observer = observer == null ? new ProcessingObserver<>() {
        } : observer;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void halt() {
        running = false;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void run() {
        while (running) {
            long nextSequence = claimNextSequence();
            try {
                while (running) {
                    long available = waitStrategy.waitFor(nextSequence, sequencer.cursorSequence(), dependentSequence);
                    long safeAvailable = sequencer.getHighestPublishedSequence(nextSequence, available);
                    if (safeAvailable < nextSequence) {
                        Thread.onSpinWait();
                        continue;
                    }
                    processSequence(nextSequence);
                    sequence.setRelease(nextSequence);
                    break;
                }
            } catch (InterruptedException interruptedException) {
                if (!running) {
                    Thread.currentThread().interrupt();
                    break;
                }
                exceptionHandler.handleOnShutdownException(interruptedException);
            } catch (Throwable ex) {
                exceptionHandler.handleOnShutdownException(ex);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processSequence(long sequenceValue) throws InterruptedException {
        Object entry = ringBuffer.get(sequenceValue);
        if (entry instanceof EventSlot<?> rawSlot) {
            handleSlot((EventSlot<E>) rawSlot, sequenceValue);
            return;
        }
        handleEvent((E) entry, sequenceValue);
    }

    private void handleSlot(EventSlot<E> slot, long sequenceValue) throws InterruptedException {
        switch (slot.state()) {
            case READY -> handleEvent(slot.event(), sequenceValue);
            case TRANSLATE_FAILED -> observer.onSkippedPublishFailure(sequenceValue, slot.event(), slot.publishError());
            case EMPTY, TRANSLATING -> safeHandleEventException(
                    new IllegalStateException("unexpected slot state: " + slot.state() + ", stage=" + stageName),
                    sequenceValue,
                    slot.event()
            );
        }
    }

    private void handleEvent(E event, long sequenceValue) throws InterruptedException {
        long startNanos = System.nanoTime();
        Throwable lastError = null;
        int maxAttempts = Math.max(1, retryPolicy.maxAttempts());

        for (int attempt = 1; running && attempt <= maxAttempts; attempt++) {
            try {
                workHandler.onEvent(event);
                observer.onSuccess(sequenceValue, event, System.nanoTime() - startNanos);
                return;
            } catch (Throwable ex) {
                lastError = ex;
                if (attempt < maxAttempts) {
                    observer.onRetry(sequenceValue, event, ex, attempt, System.nanoTime() - startNanos);
                    sleepBackoff(retryPolicy.backoffMillis(attempt));
                }
            }
        }

        if (lastError != null) {
            safeHandleEventException(lastError, sequenceValue, event);
            try {
                deadLetterHandler.onDeadLetter(new DeadLetterEvent<>(
                        stageName,
                        sequenceValue,
                        event,
                        lastError,
                        maxAttempts,
                        System.currentTimeMillis()
                ));
                observer.onDeadLetter(sequenceValue, event, lastError, maxAttempts, System.nanoTime() - startNanos);
            } catch (Throwable ex) {
                exceptionHandler.handleOnShutdownException(ex);
            }
            observer.onFailure(sequenceValue, event, lastError, System.nanoTime() - startNanos);
        }
    }

    private long claimNextSequence() {
        long nextSequence;
        do {
            nextSequence = workSequence.getVolatile() + 1;
            sequence.setRelease(nextSequence - 1);
        } while (!workSequence.compareAndSet(nextSequence - 1, nextSequence));
        return nextSequence;
    }

    private void sleepBackoff(long backoffMillis) throws InterruptedException {
        if (backoffMillis <= 0L) {
            return;
        }
        Thread.sleep(backoffMillis);
    }

    private void safeHandleEventException(Throwable ex, long sequenceValue, E event) {
        try {
            exceptionHandler.handleEventException(ex, sequenceValue, event);
        } catch (Throwable handlerEx) {
            exceptionHandler.handleOnShutdownException(handlerEx);
        }
    }
}
