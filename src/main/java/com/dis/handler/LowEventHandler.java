package com.dis.handler;

import com.dis.OrderEvent;

// 简单日志输出 handler，主要用于演示。
public class LowEventHandler implements EventHandler<OrderEvent> {

    @Override
    public void onEvent(OrderEvent event, long sequence) {
        System.out.println(event);
    }
}
