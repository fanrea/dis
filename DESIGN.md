# Dis 设计说明

## 发布失败为什么仍然推进 sequence

多生产者 RingBuffer 要求 sequence 连续推进。translator 抛异常后如果不 publish 当前 sequence，后续已经成功写入的事件也会被消费者阻塞在空洞之前。因此引擎仍然 publish sequence，但把槽位标记为 `TRANSLATE_FAILED`，消费者只记录跳过，不调用业务 handler。

## EventSlot 状态流转

`DefaultEventEngine` 内部使用 `RingBuffer<EventSlot<E>>`。槽位状态从 `EMPTY` 进入 `TRANSLATING`，translator 成功后变为 `READY`，失败后变为 `TRANSLATE_FAILED`。业务 handler 只处理 `READY` 事件。

## RingBuffer 复用与 EventResetter

RingBuffer 复用事件对象可以减少 GC，但旧字段可能残留。每次 translator 执行前都会调用 `EventResetter<E>`，业务可以通过 `OrderEvent::reset` 清理所有字段。

## publishEvent 与 tryPublishEvent

`publishEvent` 保留原阻塞语义，队列满时等待可用 sequence。`tryPublishEvent` 在超时时返回 `false`，不调用 translator，也不会产生事件，用于高峰期降级或快速失败。

## shutdown 与 shutdownGracefully

`shutdown` 是快速停止，会 halt processor。`shutdownGracefully` 先进入 `DRAINING`，拒绝新发布，再等待已发布事件被所有消费者追上，最后停止线程。超时时返回 `false`，不强杀线程，调用方仍可再执行 `shutdown`。

## BatchEventProcessor 与 WorkerPool

Batch 模式是广播处理：同一 stage 的每个 handler 都会处理每个事件。WorkerPool 模式是竞争消费：多个 worker 共享 `workSequence`，每个事件只被其中一个 worker 处理。WorkerPool stage 的输出依赖是所有 worker sequence 的最小值，下游 stage 会等待整个 pool 追上。

## retry 与 dead letter

handler 失败后按 `RetryPolicy` 在本地重试。重试期间当前消费者 sequence 不推进。达到最大次数后调用 `ExceptionHandler` 和 `DeadLetterHandler`，记录死信指标，然后推进 sequence，避免整条消费链卡死。

## 可观测性

`metricsSnapshot` 暴露发布成功、翻译失败、发布超时、跳过失败发布、重试、死信、优雅停机等引擎指标。`StageMetricsSnapshot` 暴露成功、错误、重试、死信、跳过、最近错误和最后处理序号。`healthReport` 基于积压、线程存活和失败信号给出健康状态。
