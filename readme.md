Dis：基于 Java 17 的轻量级高性能事件处理引擎
自研 mini Disruptor-like 事件引擎，基于 RingBuffer、Sequence、CAS 与 WaitStrategy 实现多生产者发布、批量消费和多阶段处理链编排。
设计 availableBuffer + flag 机制解决多生产者乱序发布下的连续可消费判定问题，避免消费者读取未完成发布的事件。
支持 BlockingWaitStrategy / YieldingWaitStrategy 两种等待策略，并通过 metrics snapshot 统计发布延迟、消费者进度、stage lag 和健康状态。
基于 JUnit/JMH 补充并发正确性测试与性能基准，对比 BlockingQueue 在不同 producer/consumer 配置下的吞吐和 p99 延迟表现。



总体要求：

保持 Java 17。
不要引入 Spring。
不要引入重型外部依赖。
优先保持现有 public API 兼容，确实需要新增 API 时用重载或 default method。
现有 demo 必须能继续运行。
改完后 mvn clean package 能通过。
代码风格保持当前项目简洁风格，注释用中文。
重点修改业务语义、运行时行为和框架能力，不要只补测试。

一、修复发布失败仍然 publish 脏事件的问题

当前 DefaultEventEngine.publishInternal 中，sequencer.next() 后执行 translator.translateTo(event, sequence)，如果 translator 抛异常，finally 仍然 sequencer.publish(sequence)。这会导致消费者可能消费半初始化事件。

请改成“序号必须连续，但业务 handler 不能处理脏事件”的语义。

实现要求：

新增 com.dis.core.EventSlot<E>。
EventSlot 内部包含：
E event
long sequence
EventSlotState state
Throwable publishError
long publishNanos
long visibleNanos

新增 com.dis.core.EventSlotState，至少包含：
EMPTY
TRANSLATING
READY
TRANSLATE_FAILED

RingBuffer 不再直接存 E，而是在 DefaultEventEngine 内部使用 RingBuffer<EventSlot<E>>。
EventFactory 仍然由 EngineConfig 的 Supplier<E> 提供，对外用户仍然只感知 E。
初始化 RingBuffer 时，每个槽位创建一个 EventSlot，EventSlot 内部持有一个预分配的业务事件对象。

发布流程改成：

sequence = sequencer.next()
slot = ringBuffer.get(sequence)
slot.resetForTranslate(sequence)
try:
config.eventResetter().reset(slot.event())，如果用户没有配置 resetter，则 no-op
translator.translateTo(slot.event(), sequence)
slot.markReady()
metricsCollector.recordPublishSuccess(...)
catch:
slot.markTranslateFailed(ex)
metricsCollector.recordPublishError(...)
继续向调用方抛出异常
finally:
sequencer.publish(sequence)

消费者侧 BatchEventProcessor 读取到 EventSlot 后：
如果 state == READY，才调用业务 handler.onEvent(slot.event(), sequence)。
如果 state == TRANSLATE_FAILED，不调用业务 handler，直接跳过该 sequence，并记录为 skipped publish failure。
其他状态视为框架异常，交给 exceptionHandler 或记录错误，但不能让消费者卡死。

这个改造的核心目标：
不能因为 translator 失败导致 RingBuffer 出现不可消费的空洞。
不能让业务 handler 处理半初始化事件。
不能因为一个失败发布导致后续事件全部阻塞。

二、增加事件重置机制，避免 RingBuffer 复用对象导致脏字段泄漏

当前 OrderEvent 只有 orderId 和 price，实际业务事件对象字段会更多。如果某次发布只设置部分字段，旧字段可能残留。

请新增接口：

com.dis.api.EventResetter<E>
void reset(E event)

EngineConfig.Builder 增加：
eventResetter(EventResetter<E> eventResetter)

默认 resetter 为 no-op。

在每次 translator.translateTo 前调用 resetter.reset(event)。

修改 OrderEvent：
增加更多业务字段：
orderId
userId
skuId
price
quantity
traceId
status
riskPassed
stockReserved
createdAtMillis
errorCode
errorMessage

给 OrderEvent 增加 reset() 方法，把所有字段恢复默认值。

在 demo 的 EngineConfig 中配置 OrderEvent::reset。

目标：
面试时可以解释 RingBuffer 复用对象虽然减少 GC，但必须处理字段残留问题。

三、增加发布背压语义：支持阻塞发布和限时发布

当前 publisher.publishEvent 只能一直等 sequencer.next()，业务线程在队列满时可能无限阻塞。

请增加限时发布能力。

修改 EventPublisher：

保留：
void publishEvent(EventTranslator<E> translator)

新增：
boolean tryPublishEvent(EventTranslator<E> translator, long timeout, TimeUnit unit)

语义：
publishEvent 维持原有行为，队列满时阻塞等待。
tryPublishEvent 在 timeout 内拿不到可用 sequence，返回 false，不调用 translator，不产生事件。
tryPublishEvent 如果拿到 sequence，但 translator 失败，仍然按第一部分的发布失败语义处理，并向调用方抛异常。

修改 Sequencer 接口：
新增：
long tryNext(long timeout, TimeUnit unit)

返回值语义：
成功返回 sequence。
超时返回 -1。

MultiProducerSequencer 实现 tryNext：
复用 next() 的容量判断逻辑。
不能无限 park。
超时后返回 -1。
注意纳秒时间计算，不要因为 System.nanoTime 溢出写出错误逻辑。

DefaultEventEngine.publishInternal 拆成：
publishInternal(EventTranslator<E> translator)
tryPublishInternal(EventTranslator<E> translator, long timeout, TimeUnit unit)

目标：
业务侧可以在高峰期选择丢弃、降级、返回繁忙，而不是无限阻塞。

四、增加优雅停机 drain 语义

当前 shutdown 会 halt 所有 processor，可能导致已经发布但还没处理完的事件被丢弃。请增加 graceful shutdown。

EventEngine 新增：
void shutdownGracefully()
boolean shutdownGracefully(long timeout, TimeUnit unit) throws InterruptedException

语义：
shutdown() 保持原语义：快速停止。
shutdownGracefully()：

1. EngineState 从 STARTED 改为 DRAINING。
2. DRAINING 后拒绝新发布，publishEvent 和 tryPublishEvent 都抛 IllegalStateException。
3. 等待所有消费者 sequence 追上当前 cursor。
4. 追上后再 halt processor。
5. 等待 worker 线程退出。
6. 状态置为 SHUTDOWN。

如果超时：
返回 false。
不强制 kill 线程。
用户仍然可以再调用 shutdown() 快速终止。

EngineState 增加：
DRAINING

DefaultEventEngine.healthReport / metricsSnapshot 能体现 DRAINING 状态。

目标：
业务语义上区分快速停止和“处理完已提交事件再停机”。

五、增加失败处理策略：本地重试 + 死信队列

当前 BatchEventProcessor 中 handler 抛异常后只是 exceptionHandler.handleEventException，然后继续推进 sequence。这个语义太粗，业务上不够完整。

请增加可配置的失败处理策略。

新增 com.dis.api.RetryPolicy：

字段/方法建议：
int maxAttempts()
long backoffMillis(int attempt)

提供静态工厂：
RetryPolicy.noRetry()
RetryPolicy.fixedBackoff(int maxAttempts, long backoffMillis)

注意 attempts 语义：
maxAttempts 包含第一次执行。
noRetry 等价于 maxAttempts = 1。

EngineConfig.Builder 增加：
retryPolicy(RetryPolicy retryPolicy)

默认 noRetry。

新增 DeadLetterEvent<E>：
String stageName
long sequence
E eventSnapshotOrReference
Throwable cause
int attempts
long deadLetterAtMillis

新增 DeadLetterHandler<E>：
void onDeadLetter(DeadLetterEvent<E> event)

EngineConfig.Builder 增加：
deadLetterHandler(DeadLetterHandler<E> deadLetterHandler)

默认实现：
输出到 System.err，不能中断消费线程。

BatchEventProcessor 处理 READY 事件时：
按 retryPolicy 执行业务 handler。
如果 handler 第一次失败，且还有重试次数，则 sleep backoffMillis(attempt)，然后重试。
如果所有重试失败：
调用 exceptionHandler.handleEventException。
构造 DeadLetterEvent，调用 deadLetterHandler.onDeadLetter。
然后推进 sequence，避免整个消费链卡死。

要求：
重试期间不要推进当前消费者 sequence。
重试失败后必须推进 sequence。
DeadLetterHandler 自身抛异常不能杀死消费者线程，应捕获并交给 exceptionHandler.handleOnShutdownException 或打印错误。

目标：
让项目具备真实业务失败语义：失败可重试，重试后仍失败进入死信，而不是静默跳过。

六、给处理链增加阶段名称，提升业务可观测性

当前 stageName 是 stage-1-handler-1 这种自动名，不利于简历和业务理解。

EventEngine 增加重载：
EventChain handleEventsWith(String stageName, BusinessEventHandler<E>... handlers)

EventChain 增加重载：
EventChain then(String stageName, BusinessEventHandler<E>... handlers)

原有 handleEventsWith(...) 和 then(...) 保持可用，内部仍然生成默认 stageName。

DefaultEventEngine.appendStage 支持传入 stageNamePrefix。
如果一个 stage 有多个 handler，命名为：
{stageName}-handler-1
{stageName}-handler-2

demo 中使用真实业务阶段名：
validate-order
risk-check
reserve-stock
send-notification

metricsSnapshot 和 healthReport 中展示这些阶段名。

目标：
让项目从“抽象并发 demo”变成“可解释的业务事件处理链”。

七、暴露 WorkProcessor 对应的 worker pool 业务能力

当前项目里有 WorkProcessor，但 DefaultEventEngine 没有暴露 worker pool API，导致这个能力像半成品。

请补齐 worker pool 模式。

EventEngine 新增：
EventChain handleEventsWithWorkerPool(String stageName, int workerCount, WorkHandler<E> handler)

EventChain 新增：
EventChain thenWorkerPool(String stageName, int workerCount, WorkHandler<E> handler)

语义：
BatchEventProcessor 模式：每个 handler 都会处理每个事件，适合广播式流水线。
WorkerPool 模式：多个 worker 抢占同一批事件，每个事件只被其中一个 worker 处理，适合并行消费。

DefaultEventEngine 中：
复用已有 WorkProcessor。
为同一个 worker pool 创建共享 workSequence。
每个 WorkProcessor 的 sequence 都要加入 gatingSequences。
worker pool stage 的输出依赖应该是该 pool 内所有 worker sequence 的 SequenceGroup。
后续 then 阶段必须等待 worker pool 内所有 worker 都处理到对应 sequence 之后才能继续。

注意：
WorkProcessor 当前字段 depentSequence 拼写错误，请改为 dependentSequence。
WorkProcessor 也要适配 EventSlot<E>，只有 READY 才调用 WorkHandler。
TRANSLATE_FAILED 直接跳过。
WorkProcessor 也要支持 retryPolicy 和 deadLetterHandler，或者至少不要吞异常。

目标：
简历上可以写支持广播消费和竞争消费两种模式，而不是只写了 WorkProcessor 没接入。

八、增加业务 demo：订单异步处理流水线

重写或新增一个 demo，名字建议：
com.dis.demo.OrderPipelineDemo

业务场景：
模拟订单创建后的异步处理链。

事件字段使用增强后的 OrderEvent。

发布阶段：
生成 orderId、userId、skuId、price、quantity、traceId、createdAtMillis。
随机制造少量异常订单，例如 price <= 0 或 skuId 为空。

处理链：
validate-order：
校验 orderId、userId、skuId、price、quantity。
失败抛异常。

risk-check：
模拟风控。
例如 userId 命中某个规则时 riskPassed=false 并抛异常，或者通过则 riskPassed=true。

reserve-stock：
使用 worker pool，workerCount=2 或 4。
模拟库存预占。
成功则 stockReserved=true。

send-notification：
只有 riskPassed && stockReserved 才输出通知日志。
否则输出跳过原因。

配置：
bufferSize 1024。
waitStrategy 使用 BlockingWaitStrategy。
eventResetter 使用 OrderEvent::reset。
retryPolicy 使用 fixedBackoff(3, 10)。
deadLetterHandler 打印 stageName、sequence、orderId、cause。
启用 observabilityLog。

demo 需要展示：
正常事件完整通过 validate -> risk -> stock -> notification。
异常事件进入 retry，然后进入 dead letter。
发布失败不会进入业务 handler。
shutdownGracefully 能处理完已发布事件。

目标：
项目不再只是“高性能队列”，而是有一个清晰业务场景：订单异步处理流水线。

九、补充业务状态语义，不要让 handler 只能靠异常表达所有结果

当前 BusinessEventHandler 只能 void onEvent，业务失败只能抛异常。可以保留该 API，但在 OrderEvent demo 中通过 status 字段表达业务状态。

OrderEvent 增加 enum OrderEventStatus：
NEW
VALIDATED
RISK_PASSED
RISK_REJECTED
STOCK_RESERVED
STOCK_FAILED
NOTIFIED
FAILED

各阶段 handler 负责更新 status。
异常场景设置 errorCode 和 errorMessage。
DeadLetterEvent 输出 status、errorCode、errorMessage。

目标：
让业务 demo 有状态流转，不是简单 println。

十、完善发布状态和消费状态的 metrics

现有 metrics 可以保留，但要增加以下计数：

publishTranslateFailedCount
publishTimeoutCount
consumerSkippedTranslateFailedCount
handlerRetryCount
deadLetterCount
gracefulShutdownCount
gracefulShutdownTimeoutCount

StageMetrics 增加：
successCount
errorCount
retryCount
deadLetterCount
skippedCount
lastErrorMessage
lastProcessedSequence

这些字段要能从 metricsSnapshot 看到。

目标：
业务语义改造后，可观测性也能解释这些状态，不是只看 cursor 和 lag。

十一、修改 README 或新增 DESIGN.md

本次可以顺手补一个简短 DESIGN.md，重点解释业务语义，不需要写长篇性能报告。

内容包括：

1. 为什么 translator 失败仍要 publish sequence，但不能让业务 handler 处理脏事件。
2. EventSlot 的状态流转。
3. RingBuffer 复用对象为什么需要 EventResetter。
4. publishEvent 和 tryPublishEvent 的区别。
5. shutdown 和 shutdownGracefully 的区别。
6. BatchEventProcessor 和 WorkerPool 的区别。
7. retry + dead letter 的业务语义。

目标：
让面试官点进仓库能快速看懂你解决了哪些真实工程问题。

十二、验收标准

完成后需要满足：

mvn clean package 通过。
现有 API 调用方式尽量不破坏。
OrderPipelineDemo 可以直接运行。
OrderPipelineDemo 输出中能看到：
正常订单通过完整链路。
异常订单重试。
重试失败进入 dead letter。
限时发布失败时返回 false。
shutdownGracefully 等待已发布事件处理完成。
metricsSnapshot 能看到新增计数。
healthReport 不因新增状态报错。
代码中不出现 TODO、临时 debug、硬编码 sleep 大量等待。
不要提交 IDE 文件、编译产物或无关目录。

十三、简历导向的最终效果

改完后，这个项目应该能支撑下面这些简历表述：

基于 Java 17 自研轻量级事件处理引擎，支持多生产者 RingBuffer 发布、广播式处理链和 worker pool 竞争消费模式。
设计 EventSlot 状态机解决发布填充失败导致的脏事件消费问题，同时保证 sequence 连续推进，避免消费者阻塞。
引入 EventResetter 解决 RingBuffer 对象复用下的字段残留问题。
支持阻塞发布和限时发布，提供队列满时的业务降级能力。
实现 graceful shutdown，支持拒绝新事件并等待已发布事件处理完成。
实现 handler retry 和 dead letter 机制，完善业务失败处理语义。
通过阶段命名和 metrics/health report 暴露 stage lag、retry、dead letter、publish timeout 等运行时状态。
