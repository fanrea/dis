package com.dis.core;

import com.dis.api.DeadLetterEvent;
import com.dis.api.DeadLetterHandler;
import com.dis.api.RetryPolicy;
import com.dis.handler.EventHandler;
import com.dis.handler.ExceptionHandler;
import com.dis.strategy.WaitStrategy;

/**
 * 批量事件处理器。
 *
 * 处理流程：
 * 1. 等待可用序号。
 * 2. 计算连续可消费上界。
 * 3. 批量回调业务处理器。
 * 4. 推进当前消费者序号。
 */
public final class BatchEventProcessor<E> implements Runnable {
    private final RingBuffer<?> ringBuffer;
    private final Sequencer sequencer;
    private final Sequence sequence = new Sequence(-1);
    private final Sequence dependentSequence;
    private final EventHandler<E> handler;
    private final ExceptionHandler<E> exceptionHandler;
    private final WaitStrategy waitStrategy;
    private final RetryPolicy retryPolicy;
    private final DeadLetterHandler<E> deadLetterHandler;
    private final ProcessingObserver<E> observer;
    private final String stageName;

    private volatile boolean running = true;

    public BatchEventProcessor(RingBuffer<?> ringBuffer,
                               Sequencer sequencer,
                               Sequence dependentSequence,
                               WaitStrategy waitStrategy,
                               EventHandler<E> handler,
                               ExceptionHandler<E> exceptionHandler) {
        this(ringBuffer, sequencer, dependentSequence, waitStrategy, handler, exceptionHandler,
                RetryPolicy.noRetry(), event -> {
                }, "stage", new ProcessingObserver<>() {
        });
    }

    public BatchEventProcessor(RingBuffer<?> ringBuffer,
                               Sequencer sequencer,
                               Sequence dependentSequence,
                               WaitStrategy waitStrategy,
                               EventHandler<E> handler,
                               ExceptionHandler<E> exceptionHandler,
                               RetryPolicy retryPolicy,
                               DeadLetterHandler<E> deadLetterHandler,
                               String stageName,
                               ProcessingObserver<E> observer) {
        this.ringBuffer = ringBuffer;
        this.sequencer = sequencer;
        this.dependentSequence = dependentSequence != null ? dependentSequence : sequencer.cursorSequence();
        this.waitStrategy = waitStrategy;
        this.handler = handler;
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

    // 请求停止处理器，并唤醒等待线程。
    public void halt() {
        running = false;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void run() {
        long nextSeq = sequence.getVolatile() + 1;

        while (running) {
            try {
                long available = waitStrategy.waitFor(nextSeq, sequencer.cursorSequence(), dependentSequence);
                long safeAvailable = sequencer.getHighestPublishedSequence(nextSeq, available);

                while (nextSeq <= safeAvailable) {
                    processSequence(nextSeq);
                    nextSeq++;
                }

                sequence.setRelease(nextSeq - 1);
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
    private void processSequence(long nextSeq) throws InterruptedException {
        Object entry = ringBuffer.get(nextSeq);
        if (entry instanceof EventSlot<?> rawSlot) {
            handleSlot((EventSlot<E>) rawSlot, nextSeq);
            return;
        }

        handleEvent((E) entry, nextSeq);
    }

    private void handleSlot(EventSlot<E> slot, long sequenceValue) throws InterruptedException {
        switch (slot.state()) {
            case READY -> handleEvent(slot.event(), sequenceValue);
            case TRANSLATE_FAILED -> {
                Throwable cause = slot.publishError();
                observer.onSkippedPublishFailure(sequenceValue, slot.event(), cause);
            }
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
                handler.onEvent(event, sequenceValue);
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
