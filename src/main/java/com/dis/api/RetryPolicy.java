package com.dis.api;

// 重试策略。
public interface RetryPolicy {
    // 最大尝试次数（包含首次执行）。
    int maxAttempts();

    // 第 attempt 次失败后的回退时间（毫秒）。
    long backoffMillis(int attempt);

    static RetryPolicy noRetry() {
        return fixedBackoff(1, 0L);
    }

    static RetryPolicy fixedBackoff(int maxAttempts, long backoffMillis) {
        return new DefaultRetryPolicy(maxAttempts, backoffMillis);
    }

    record DefaultRetryPolicy(int maxAttempts, long backoffMillis) implements RetryPolicy {
        public DefaultRetryPolicy {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("最大尝试次数必须大于等于 1");
            }
            if (backoffMillis < 0) {
                throw new IllegalArgumentException("回退毫秒数必须大于等于 0");
            }
        }

        @Override
        public long backoffMillis(int attempt) {
            if (attempt < 1) {
                throw new IllegalArgumentException("尝试次数必须大于等于 1");
            }
            return backoffMillis;
        }
    }
}
