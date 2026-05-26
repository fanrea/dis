package com.dis.api;

// 发布阶段的对象填充器。
// 约定：
// 1. 仅填充当前事件，不做阻塞操作。
// 2. 抛出异常会标记该 sequence 为 TRANSLATE_FAILED。
@FunctionalInterface
public interface EventTranslator<E> {
    void translateTo(E event, long sequence);
}
