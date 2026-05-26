package com.dis.core;

import com.dis.strategy.WaitStrategy;
import com.dis.strategy.PublishSignalPolicy;
import com.dis.strategy.AlwaysSignalPolicy;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

// 多生产者序列分配器，负责“申请写入权”和“发布可见性”。
// 核心逻辑：
// 1. cursor 表示已 claim 的最大序号，生产者拿到 sequence 后仍要写槽位并调用 publish。
// 2. 多生产者可能乱序发布，例如 10 还在写、11 已写完，所以消费者不能只看 cursor。
// 3. availableBuffer + flag 记录每个槽位当前发布的是哪一轮 sequence，防止读到上一轮残留。
// 4. gating sequence 表示消费者处理进度，生产者 claim 前必须确认不会覆盖未消费槽位。
public final class MultiProducerSequencer implements Sequencer {
    private final int bufferSize; // RingBuffer 容量。
    private final WaitStrategy waitStrategy; // 生产者等待空间、消费者等待事件时共用的等待策略。
    private final PublishSignalPolicy publishSignalPolicy; // publish 后是否需要唤醒消费者。
    private final Sequence cursor = new Sequence(-1); // 已被生产者认领到的最大 sequence，不代表都已发布完成。
    private volatile Sequence[] gatingSequences; // 所有消费者进度，生产者用它判断 RingBuffer 是否还有可写空间。

    // 按槽位记录发布轮次 flag，消费者据此判断可见性。
    // 同一个数组槽位会被 sequence、sequence + bufferSize、sequence + 2*bufferSize 复用，
    // 因此不能只记录 boolean，必须记录轮次 flag 才能区分本轮发布和上一轮残留。
    private final int[] availableBuffer; // 按槽位保存已发布轮次。
    private final int indexMask; // sequence 到数组下标的位掩码。
    private final int indexShift; // sequence 右移后得到发布轮次 flag。

    private static final VarHandle AVAILABLE_VH = MethodHandles.arrayElementVarHandle(int[].class); // availableBuffer 的 VarHandle。

    public MultiProducerSequencer(int bufferSize, WaitStrategy waitStrategy, Sequence... gatingSequences) {
        this(bufferSize, waitStrategy, new AlwaysSignalPolicy(), gatingSequences);
    }

    public MultiProducerSequencer(int bufferSize,
                                  WaitStrategy waitStrategy,
                                  PublishSignalPolicy publishSignalPolicy,
                                  Sequence... gatingSequences) {
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("缓冲区大小必须是 2 的幂");
        }
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;
        this.publishSignalPolicy = Objects.requireNonNull(publishSignalPolicy, "publishSignalPolicy");
        this.gatingSequences = gatingSequences == null ? new Sequence[0] : Arrays.copyOf(gatingSequences, gatingSequences.length);
        this.availableBuffer = new int[bufferSize];
        this.indexMask = bufferSize - 1;
        this.indexShift = Integer.numberOfTrailingZeros(bufferSize);

        for (int i = 0; i < bufferSize; i++) {
            AVAILABLE_VH.set(availableBuffer, i, -1);
        }
    }

    @Override
    public long next() {
        return claimNext(false, 0L, 0L);
    }

    @Override
    public long tryNext(long timeout, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        if (timeout < 0) {
            throw new IllegalArgumentException("超时时间必须大于等于 0");
        }
        long timeoutNanos = unit.toNanos(timeout);
        if (timeout > 0 && timeoutNanos <= 0) {
            timeoutNanos = Long.MAX_VALUE;
        }
        long start = System.nanoTime();
        return claimNext(true, timeoutNanos, start);
    }

    // 申请一个 sequence。
    // claim 只负责分配写入权，不负责发布可见性。
    // 多生产者通过 CAS 竞争 cursor：
    // 1. 先根据 cursor 推导下一个候选 sequence。
    // 2. 如果候选 sequence 会绕回覆盖消费者未处理的槽位，就等待 gating sequence 前进。
    // 3. CAS 成功后返回 sequence，调用方填充 RingBuffer 槽位，再调用 publish(sequence)。
    private long claimNext(boolean timed, long timeoutNanos, long startTime) {
        // 本地缓存最慢消费者进度，只有在接近环绕时才重新扫描 gatingSequences，减少热点路径开销。
        long cachedGatingSequence = cursor.getVolatile() - bufferSize;//只是初始化一下,让第一次先走一次真实校验
        //如果当前线程一直cas失败导致cachedGatingSequence与current差距一直大,会一直getMinimumSequence产生消耗,
        while (true) {
            long current = cursor.getVolatile();
            long next = current + 1;
            long wrapPoint = next - bufferSize;

            if (wrapPoint > cachedGatingSequence) {
                // wrapPoint 是新 sequence 即将覆盖的旧 sequence。
                // 如果最慢消费者还没有越过 wrapPoint，说明目标槽位仍可能被消费，生产者必须等待。
                long minSequence = getMinimumSequence(gatingSequences, current);//获取实际的最新被消费位置
                if (wrapPoint > minSequence) {
                    if (timed) {
                        long elapsed = System.nanoTime() - startTime;
                        if (elapsed >= timeoutNanos) {
                            return -1L;
                        }
                        long remaining = timeoutNanos - elapsed;
                        LockSupport.parkNanos(Math.min(remaining, 1_000_000L));
                    } else {
                        LockSupport.parkNanos(1L);
                    }
                    continue;
                }
                cachedGatingSequence = minSequence;
            }

            // cursor 只推进 claim 进度。即使 CAS 成功，消费者也要等 publish 后才能看到该 sequence。
            if (cursor.compareAndSet(current, next)) {
                return next;
            }
        }
    }

    @Override
    public void publish(long sequence) {
        int index = (int) (sequence & indexMask);
        int flag = (int) (sequence >>> indexShift);

        // release-store 建立发布屏障：
        // 调用对方在 publish 前事件对象或 EventSlot 状态的写入，对消费者的 getAcquire 可见。
        AVAILABLE_VH.setRelease(availableBuffer, index, flag);
        if (publishSignalPolicy.shouldSignal(sequence)) {
            waitStrategy.signalAllWhenBlocking();
        }
    }

    @Override
    public Sequence cursorSequence() {
        return cursor;
    }

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence) {
        // waitStrategy 只能告诉消费者 cursor/dependency 已经走到哪里，不能证明中间所有 sequence 都已发布。
        // 这里从 lowerBound 开始逐个检查发布 flag，只返回连续可消费上界，遇到空洞立即停止。
        for (long sequence = lowerBound; sequence <= availableSequence; sequence++) {
            int index = (int) (sequence & indexMask);
            int flag = (int) (sequence >>> indexShift);
            if ((int) AVAILABLE_VH.getAcquire(availableBuffer, index) != flag) {
                return sequence - 1;
            }
        }
        return availableSequence;
    }

    private static long getMinimumSequence(Sequence[] sequences, long defaultValue) {
        // 没有消费者时使用当前 cursor 作为默认值，表示生产者无需等待消费进度。
        long min = Long.MAX_VALUE;
        for (Sequence s : sequences) {
            long v = s.getVolatile();
            min = Math.min(min, v);
        }
        return min == Long.MAX_VALUE ? defaultValue : min;
    }

    @Override
    public synchronized void addGatingSequence(Sequence... sequencesToAdd) {
        // 配置阶段低频更新，使用 synchronized + copy-on-write 简化并发安全。
        if (sequencesToAdd == null || sequencesToAdd.length == 0) {
            return;
        }
        Sequence[] currentSequences = this.gatingSequences;
        Sequence[] updatedSequences = Arrays.copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);
        System.arraycopy(sequencesToAdd, 0, updatedSequences, currentSequences.length, sequencesToAdd.length);
        this.gatingSequences = updatedSequences;
    }
}
