package com.dis.core;

import com.dis.handler.ExceptionHandler;
import com.dis.handler.WorkHandler;
import com.dis.strategy.WaitStrategy;

/**
 * 工作池处理器。
 *
 * 与 BatchEventProcessor 不同：
 * 1. 多个 WorkProcessor 共享一个 workSequence 抢占任务。
 * 2. 每个事件只会被一个 worker 处理一次。
 */
public final class WorkProcessor<E> implements Runnable {
    private final RingBuffer<E> ringBuffer;
    private final Sequencer sequencer;
    private final WaitStrategy waitStrategy;
    private final ExceptionHandler<E> exceptionHandler;
    private final WorkHandler<E> workHandler;
    private final Sequence depentSequence;

    // 当前 worker 的处理进度。
    private final Sequence sequence = new Sequence(-1);

    // 所有 worker 共享的任务游标。
    private final Sequence workSequence;

    private volatile boolean running = true;

    public WorkProcessor(RingBuffer<E> ringBuffer,
                         Sequencer sequencer,
                         Sequence workSequence,
                         Sequence depentSequence,
                         WaitStrategy waitStrategy,
                         WorkHandler<E> workHandler,
                         ExceptionHandler<E> exceptionHandler) {
        this.ringBuffer = ringBuffer;
        this.sequencer = sequencer;
        this.workSequence = workSequence;
        this.depentSequence = depentSequence == null ? sequencer.cursorSequence() : depentSequence;
        this.waitStrategy = waitStrategy;
        this.workHandler = workHandler;
        this.exceptionHandler = exceptionHandler;
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
        boolean processedSequence = true;
        long cachedAvailableSequence = Long.MIN_VALUE;
        long nextSequence = sequence.getVolatile();
        E event = null;

        while (running) {
            try {
                if (processedSequence) {
                    processedSequence = false;
                    do {
                        nextSequence = workSequence.getVolatile() + 1;
                        sequence.setRelease(nextSequence - 1);
                    } while (!workSequence.compareAndSet(nextSequence - 1, nextSequence));
                }

                if (cachedAvailableSequence >= nextSequence) {
                    event = ringBuffer.get(nextSequence);
                    workHandler.onEvent(event);
                    processedSequence = true;
                } else {
                    long availableSequence = waitStrategy.waitFor(nextSequence, sequencer.cursorSequence(), depentSequence);
                    cachedAvailableSequence = sequencer.getHighestPublishedSequence(nextSequence, availableSequence);
                }
            } catch (Throwable ex) {
                exceptionHandler.handleEventException(ex, nextSequence, event);
                // 异常后允许继续抢占下一条，避免卡死在同一序号。
                processedSequence = true;
            }
        }
    }
}
