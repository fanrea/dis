package com.dis.api;

// 死信事件载体。
// eventSnapshotOrReference 通常是当前槽位中的事件对象引用；
// 若业务需要长期保存，请在 DeadLetterHandler 内自行做深拷贝或序列化。
public record DeadLetterEvent<E>(
        String stageName,
        long sequence,
        E eventSnapshotOrReference,
        Throwable cause,
        int attempts,
        long deadLetterAtMillis
) {
}
