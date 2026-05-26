package com.dis.core;

import com.dis.api.DeadLetterEvent;
import com.dis.api.DeadLetterHandler;
import com.dis.api.RetryPolicy;
import com.dis.handler.EventHandler;
import com.dis.handler.ExceptionHandler;
import com.dis.strategy.WaitStrategy;

// 广播型批处理消费者。
// 核心逻辑：
// 1. 一个 stage 内的每个 handler 都会处理每个事件，属于广播消费。
// 2. 每次等待上游可用后，批量处理连续可消费区间，最后统一推进自己的 sequence。
// 3. 当前 sequence 重试期间不推进进度，避免下游提前看到未完成事件。
// 4. 与 WorkProcessor 不同，这里的 sequence 属于单个 handler，表示该 handler 已完整处理到的位置。
public final class BatchEventProcessor<E> implements Runnable {
    private final RingBuffer<?> ringBuffer; // 兼容 EventSlot 包装和直接业务对象两种 RingBuffer 用法。
    private final Sequencer sequencer; // 用于读取生产者 cursor 和连续发布上界。
    private final Sequence sequence = new Sequence(-1); // 当前 handler 已完整处理到的 sequence。
    private final Sequence dependentSequence; // 上游依赖进度。
    private final EventHandler<E> handler; // 实际执行业务逻辑的 handler。
    private final ExceptionHandler<E> exceptionHandler; // 业务异常和停机异常处理器。
    private final WaitStrategy waitStrategy; // 等待上游 sequence 可用的策略。
    private final RetryPolicy retryPolicy; // 当前事件处理失败后的重试策略。
    private final DeadLetterHandler<E> deadLetterHandler; // 重试耗尽后的死信处理器。
    private final ProcessingObserver<E> observer; // 处理过程观测回调。
    private final String stageName; // stage 名称，用于指标和死信。

    private volatile boolean running = true; // 消费循环开关。

    public BatchEventProcessor(RingBuffer<?> ringBuffer,
                               Sequencer sequencer,
                               Sequence dependentSequence,
                               WaitStrategy waitStrategy,
                               EventHandler<E> handler,
                               ExceptionHandler<E> exceptionHandler) {
        this(ringBuffer, sequencer, dependentSequence, waitStrategy, handler, exceptionHandler,
                RetryPolicy.noRetry(), event -> {
                }, "阶段", new ProcessingObserver<>() {
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
        this.stageName = stageName == null || stageName.isBlank() ? "阶段" : stageName;
        this.observer = observer == null ? new ProcessingObserver<>() {
        } : observer;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void halt() {
        // 停止标记配合 signal，保证阻塞在 waitStrategy 的线程能尽快醒来退出。
        running = false;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void run() {
        // 从当前消费者进度的下一个 sequence 开始处理。
        // sequence 初始为 -1，因此第一次进入循环时处理 0。
        long nextSeq = sequence.getVolatile() + 1;

        while (running) {
            try {
                // waitStrategy 负责等待上游可用：
                // cursorSequence 表示生产者已 claim 的最大 sequence；
                // dependentSequence 表示本 stage 依赖的上游 stage 已处理到哪里。
                long available = waitStrategy.waitFor(nextSeq, sequencer.cursorSequence(), dependentSequence);
                // 多生产者下 cursor 可能包含尚未 publish 的空洞。
                // safeAvailable 会把上界收缩到真正连续可消费的位置。
                long safeAvailable = sequencer.getHighestPublishedSequence(nextSeq, available);

                // 批量处理连续区间，减少每个事件都更新 sequence 的 volatile/release 写开销。
                while (nextSeq <= safeAvailable) {
                    processSequence(nextSeq);
                    nextSeq++;
                }

                // 只有连续区间全部处理完成后才推进进度，下游据此判断依赖是否满足。
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
            // runtime 发布路径使用 EventSlot 包装业务对象，核心消费者需要先检查发布状态。
            handleSlot((EventSlot<E>) rawSlot, nextSeq);
            return;
        }

        // 兼容直接把业务对象放入 RingBuffer 的低层用法。
        handleEvent((E) entry, nextSeq);
    }

    private void handleSlot(EventSlot<E> slot, long sequenceValue) throws InterruptedException {
        switch (slot.state()) {
            case READY -> handleEvent(slot.event(), sequenceValue);
            case TRANSLATE_FAILED -> {
                // 该 sequence 已推进，但发布失败，业务处理必须跳过。
                // 跳过后外层仍会推进消费者 sequence，避免整个流水线永久卡在失败发布上。
                Throwable cause = slot.publishError();
                observer.onSkippedPublishFailure(sequenceValue, slot.event(), cause);
            }
            case EMPTY, TRANSLATING -> safeHandleEventException(
                    new IllegalStateException("槽位状态异常：" + slot.state() + "，阶段=" + stageName),
                    sequenceValue,
                    slot.event()
            );
        }
    }

    private void handleEvent(E event, long sequenceValue) throws InterruptedException {
        long startNanos = System.nanoTime();
        Throwable lastError = null;
        int maxAttempts = Math.max(1, retryPolicy.maxAttempts());

        // retry 是“同一个 sequence 的同步重试”。
        // 失败期间不推进消费者 sequence，因此下游不会看到乱序完成。
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
            // 重试耗尽后交给业务异常处理器和死信处理器。
            // 当前实现选择记录失败并继续推进，避免单条异常事件阻断后续所有事件。
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
        // 重试退避发生在当前消费者线程内，期间不会推进 sequence。
        Thread.sleep(backoffMillis);
    }

    private void safeHandleEventException(Throwable ex, long sequenceValue, E event) {
        try {
            // 异常处理器本身也可能失败，必须兜底，否则消费者线程会被异常打穿。
            exceptionHandler.handleEventException(ex, sequenceValue, event);
        } catch (Throwable handlerEx) {
            exceptionHandler.handleOnShutdownException(handlerEx);
        }
    }
}
