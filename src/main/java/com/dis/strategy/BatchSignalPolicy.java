package com.dis.strategy;

// 按固定发布批次唤醒。
// 例如 batchSize=8 表示每发布 8 个 sequence 才唤醒一次。
public final class BatchSignalPolicy implements PublishSignalPolicy {
    private final int batchSize;

    public BatchSignalPolicy(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize 必须大于 0");
        }
        this.batchSize = batchSize;
    }

    @Override
    public boolean shouldSignal(long sequence) {
        return (sequence + 1L) % batchSize == 0L;
    }
}
