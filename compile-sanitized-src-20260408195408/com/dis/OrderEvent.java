package com.dis;

// 订单事件对象，用于在 RingBuffer 中传递业务数据。
public class OrderEvent {
    private String orderId;
    private long price;

    public void setValues(String orderId, long price) {
        this.orderId = orderId;
        this.price = price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderEvent{orderId='" + orderId + "', price=" + price + "}";
    }
}
