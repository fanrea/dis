package com.dis.runtime;

import com.dis.core.Sequence;

// 多依赖序列的只读聚合视图，返回所有依赖中的最小进度。
// 用于 then 接在多个上游 handler 或 worker 后面的场景：只有所有上游都处理到某个 sequence，下游才可继续。
final class SequenceGroup extends Sequence {
    private final Sequence[] sequences; // 被聚合的上游 sequence 集合。

    SequenceGroup(Sequence... sequences) {
        super(-1);
        // 这里只保存引用，实时读取每个上游 Sequence 的当前进度。
        this.sequences = sequences;
    }

    @Override
    public long getVolatile() {
        long min = Long.MAX_VALUE;
        for (Sequence sequence : sequences) {
            // 取最慢上游作为整体进度，避免下游越过还没完成的分支。
            long value = sequence.getVolatile();
            if (value < min) {
                min = value;
            }
        }
        return min == Long.MAX_VALUE ? -1 : min;
    }

    @Override
    public long getAcquire() {
        return getVolatile();
    }

    @Override
    public void setRelease(long value) {
        // SequenceGroup 是视图，不拥有真实进度，禁止被当作普通 Sequence 写入。
        throw new UnsupportedOperationException("序列组是只读视图");
    }

    @Override
    public boolean compareAndSet(long expect, long update) {
        // 同理，聚合视图不能参与 CAS 抢占。
        throw new UnsupportedOperationException("序列组是只读视图");
    }
}
