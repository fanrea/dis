package com.dis.api;

public interface RetryPolicy {
    int maxAttempts();

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
                throw new IllegalArgumentException("maxAttempts must be >= 1");
            }
            if (backoffMillis < 0) {
                throw new IllegalArgumentException("backoffMillis must be >= 0");
            }
        }

        @Override
        public long backoffMillis(int attempt) {
            if (attempt < 1) {
                throw new IllegalArgumentException("attempt must be >= 1");
            }
            return backoffMillis;
        }
    }
}
