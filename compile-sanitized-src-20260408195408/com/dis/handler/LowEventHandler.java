package com.dis.handler;

import com.dis.OrderEvent;

public class LowEventHandler implements EventHandler<OrderEvent> {


    @Override
    public void onEvent(OrderEvent event, long sequence) throws Exception {
        System.out.println(event.toString());
    }
}
