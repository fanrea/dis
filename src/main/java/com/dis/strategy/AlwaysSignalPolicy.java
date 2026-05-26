package com.dis.strategy;

// 每次 publish 都唤醒一次。
public final class AlwaysSignalPolicy implements PublishSignalPolicy {
    @Override
    public boolean shouldSignal(long sequence) {
        return true;
    }
}
