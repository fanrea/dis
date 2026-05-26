package com.dis.core;

import com.dis.api.DeadLetterEvent;
import com.dis.api.DeadLetterHandler;
import com.dis.api.RetryPolicy;
import com.dis.handler.ExceptionHandler;
import com.dis.handler.WorkHandler;
import com.dis.strategy.WaitStrategy;

// WorkerPool 模式消费者。
// 核心逻辑：
// 1. 多个 worker 通过共享 workSequence 竞争任务，谁 CAS 成功谁处理该 sequence。
// 2. 每个 sequence 只会被一个 worker 处理，适合任务分摊。
// 3. 下游依赖整个 worker pool 时，会取所有 worker 私有 sequence 的最小值。
// 4. worker 自己的 sequence 表示它已经处理到哪里，用于背压和健康观测。
public final class WorkProcessor<E> implements Runnable {
    private final RingBuffer<?> ringBuffer; // 兼容 EventSlot 包装和直接业务对象两种 RingBuffer 用法。
    private final Sequencer sequencer; // 用于读取生产者 cursor 和连续发布上界。
    private final WaitStrategy waitStrategy; // 等待上游 sequence 可用的策略。
    private final ExceptionHandler<E> exceptionHandler; // 业务异常和停机异常处理器。
    private final WorkHandler<E> workHandler; // 实际执行业务逻辑的 worker handler。
    private final Sequence workSequence; // worker pool 内所有 worker 共享的抢任务游标。
    private final Sequence dependentSequence; // 上游依赖进度；没有上游 stage 时依赖生产者 cursor。
    private final RetryPolicy retryPolicy; // 当前事件处理失败后的重试策略。
    private final DeadLetterHandler<E> deadLetterHandler; // 重试耗尽后的死信处理器。
    private final ProcessingObserver<E> observer; // 处理过程观测回调。
    private final String stageName; // stage 名称，用于指标和死信。
    private final Sequence sequence = new Sequence(-1); // 当前 worker 自己的完成进度，和共享 workSequence 是两套概念。

    private volatile boolean running = true; // 消费循环开关。

    public WorkProcessor(RingBuffer<?> ringBuffer,
                         Sequencer sequencer,
                         Sequence workSequence,
                         Sequence dependentSequence,
                         WaitStrategy waitStrategy,
                         WorkHandler<E> workHandler,
                         ExceptionHandler<E> exceptionHandler) {
        this(ringBuffer, sequencer, workSequence, dependentSequence, waitStrategy, workHandler, exceptionHandler,
                RetryPolicy.noRetry(), event -> {
                }, "阶段", new ProcessingObserver<>() {
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
        this.stageName = stageName == null || stageName.isBlank() ? "阶段" : stageName;
        this.observer = observer == null ? new ProcessingObserver<>() {
        } : observer;
    }

    public Sequence getSequence() {
        return sequence;
    }

    public void halt() {
        // 通知 run 循环退出，并唤醒可能阻塞在 waitStrategy 上的线程。
        running = false;
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void run() {
        while (running) {
            // 先从共享 workSequence 抢占一个逻辑任务号。
            // 抢到后，该 sequence 只会由当前 worker 处理。
            long nextSequence = claimNextSequence();
            try {
                while (running) {
                    // 抢任务只说明“我负责这个 sequence”，不说明生产者已经发布成功。
                    // 仍然必须等待上游依赖和 sequencer 发布可见性。
                    long available = waitStrategy.waitFor(nextSequence, sequencer.cursorSequence(), dependentSequence);
                    long safeAvailable = sequencer.getHighestPublishedSequence(nextSequence, available);
                    if (safeAvailable < nextSequence) {
                        // 该任务号已经被抢到，但对应事件尚未连续发布完成。
                        // 短暂让出 CPU pipeline，等待 publish 补齐空洞。
                        Thread.onSpinWait();
                        continue;
                    }
                    processSequence(nextSequence);
                    // 当前 worker 完成自己的任务后推进私有 sequence。
                    // 下游如果依赖整个 worker pool，会以所有 worker 的最小进度作为可用上界。
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
            // 与广播消费者一致：EventSlot 需要先检查 translator 是否成功。
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

        // worker pool 的重试同样是针对同一个 sequence 原地重试，不会把任务交给其他 worker。
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
            // 重试耗尽后记录异常和死信，然后推进当前 worker 的进度，避免单条坏事件阻塞整个 worker。
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
            // 先声明当前 worker 已完成到 nextSequence - 1，再竞争新任务。
            //
            // 这样做的目的：
            // 1. 当前 worker 在等待新任务期间，不会把下游 gating 卡在更旧的位置。
            // 2. CAS 成功后 workSequence 立即变成新任务号，其他 worker 会竞争后续 sequence。
            // 3. 如果 CAS 失败，说明其他 worker 抢先拿走了该任务，本 worker 重新读取并尝试下一个。
            sequence.setRelease(nextSequence - 1);
        } while (!workSequence.compareAndSet(nextSequence - 1, nextSequence));
        return nextSequence;
    }

    private void sleepBackoff(long backoffMillis) throws InterruptedException {
        if (backoffMillis <= 0L) {
            return;
        }
        // 退避期间当前 worker 暂停，其他 worker 仍可继续竞争后续任务。
        Thread.sleep(backoffMillis);
    }

    private void safeHandleEventException(Throwable ex, long sequenceValue, E event) {
        try {
            // 异常处理器失败时不能让 worker 线程直接退出，统一交给 shutdown 异常入口兜底。
            exceptionHandler.handleEventException(ex, sequenceValue, event);
        } catch (Throwable handlerEx) {
            exceptionHandler.handleOnShutdownException(handlerEx);
        }
    }
}
