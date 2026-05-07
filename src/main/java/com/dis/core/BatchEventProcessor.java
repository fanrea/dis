package com.dis.core;

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
    private final RingBuffer<E> ringBuffer;
    private final Sequencer sequencer;
    private final Sequence sequence = new Sequence(-1);
    private final Sequence dependentSequence;
    private final EventHandler<E> handler;
    private final ExceptionHandler<E> exceptionHandler;
    private final WaitStrategy waitStrategy;

    private volatile boolean running = true;

    public BatchEventProcessor(RingBuffer<E> ringBuffer,
                               Sequencer sequencer,
                               Sequence dependentSequence,
                               WaitStrategy waitStrategy,
                               EventHandler<E> handler,
                               ExceptionHandler<E> exceptionHandler) {
        this.ringBuffer = ringBuffer;
        this.sequencer = sequencer;
        this.dependentSequence = dependentSequence != null ? dependentSequence : sequencer.cursorSequence();
        this.waitStrategy = waitStrategy;
        this.handler = handler;
        this.exceptionHandler = exceptionHandler;
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
                    E evt = ringBuffer.get(nextSeq);
                    try {
                        handler.onEvent(evt, nextSeq);
                    } catch (Throwable ex) {
                        exceptionHandler.handleEventException(ex, nextSeq, evt);
                    }
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
}
