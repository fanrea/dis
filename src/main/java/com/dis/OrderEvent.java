package com.dis;

// 订单事件对象，在 RingBuffer 中复用传递。
public class OrderEvent {
    private String orderId;
    private String userId;
    private String skuId;
    private long price;
    private long quantity;
    private String traceId;
    private OrderEventStatus orderStatus;
    private boolean riskPassed;
    private boolean stockReserved;
    private long createdAtMillis;
    private String errorCode;
    private String errorMessage;

    public void setValues(String orderId, long price) {
        this.orderId = orderId;
        this.price = price;
    }

    // 供 EventResetter 调用，清理上次事件残留字段。
    public void reset() {
        orderId = null;
        userId = null;
        skuId = null;
        price = 0L;
        quantity = 0L;
        traceId = null;
        orderStatus = null;
        riskPassed = false;
        stockReserved = false;
        createdAtMillis = 0L;
        errorCode = null;
        errorMessage = null;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getSkuId() {
        return skuId;
    }

    public void setSkuId(String skuId) {
        this.skuId = skuId;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public long getQuantity() {
        return quantity;
    }

    public void setQuantity(long quantity) {
        this.quantity = quantity;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public OrderEventStatus getOrderStatus() {
        return orderStatus;
    }

    public void setOrderStatus(OrderEventStatus orderStatus) {
        this.orderStatus = orderStatus;
    }

    public String getStatus() {
        return orderStatus == null ? null : orderStatus.name();
    }

    public void setStatus(String status) {
        this.orderStatus = status == null ? null : OrderEventStatus.valueOf(status);
    }

    public boolean isRiskPassed() {
        return riskPassed;
    }

    public void setRiskPassed(boolean riskPassed) {
        this.riskPassed = riskPassed;
    }

    public boolean isStockReserved() {
        return stockReserved;
    }

    public void setStockReserved(boolean stockReserved) {
        this.stockReserved = stockReserved;
    }

    public long getCreatedAtMillis() {
        return createdAtMillis;
    }

    public void setCreatedAtMillis(long createdAtMillis) {
        this.createdAtMillis = createdAtMillis;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "订单事件{"
                + "订单ID='" + orderId + '\''
                + ", 用户ID='" + userId + '\''
                + ", 商品ID='" + skuId + '\''
                + ", 价格=" + price
                + ", 数量=" + quantity
                + ", 链路ID='" + traceId + '\''
                + ", 订单状态=" + orderStatus
                + ", 风控通过=" + riskPassed
                + ", 库存已预占=" + stockReserved
                + ", 创建时间毫秒=" + createdAtMillis
                + ", 错误码='" + errorCode + '\''
                + ", 错误信息='" + errorMessage + '\''
                + '}';
    }
}
