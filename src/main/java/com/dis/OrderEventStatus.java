package com.dis;

// 订单处理状态机。
public enum OrderEventStatus {
    NEW,
    VALIDATED,
    RISK_PASSED,
    RISK_REJECTED,
    STOCK_RESERVED,
    STOCK_FAILED,
    NOTIFIED,
    FAILED
}
