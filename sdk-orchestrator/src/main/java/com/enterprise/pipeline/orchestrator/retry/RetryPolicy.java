package com.enterprise.pipeline.orchestrator.retry;

import java.time.Duration;

/**
 * Retry Policy
 *
 * Defines retry behavior for failed jobs:
 * - Maximum retry attempts
 * - Delay between retries
 * - Backoff strategy (fixed, exponential, linear)
 *
 * @author Enterprise Pipeline Platform
 */
public class RetryPolicy {

    private final int maxAttempts;
    private final BackoffStrategy backoffStrategy;
    private final Duration initialDelay;
    private final Duration maxDelay;

    private RetryPolicy(int maxAttempts, BackoffStrategy backoffStrategy,
                       Duration initialDelay, Duration maxDelay) {
        this.maxAttempts = maxAttempts;
        this.backoffStrategy = backoffStrategy;
        this.initialDelay = initialDelay;
        this.maxDelay = maxDelay;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    /**
     * Get retry delay for given attempt number
     */
    public Duration getRetryDelay(int attemptNumber) {
        if (attemptNumber <= 0) {
            return Duration.ZERO;
        }

        Duration delay = switch (backoffStrategy) {
            case FIXED -> initialDelay;
            case LINEAR -> initialDelay.multipliedBy(attemptNumber);
            case EXPONENTIAL -> initialDelay.multipliedBy((long) Math.pow(2, attemptNumber - 1));
        };

        // Cap at max delay
        return delay.compareTo(maxDelay) > 0 ? maxDelay : delay;
    }

    /**
     * No retry policy
     */
    public static RetryPolicy noRetry() {
        return new RetryPolicy(1, BackoffStrategy.FIXED, Duration.ZERO, Duration.ZERO);
    }

    /**
     * Fixed delay retry
     */
    public static RetryPolicy fixedDelay(int maxAttempts, Duration delay) {
        return new RetryPolicy(maxAttempts, BackoffStrategy.FIXED, delay, delay);
    }

    /**
     * Exponential backoff retry
     */
    public static RetryPolicy exponentialBackoff(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        return new RetryPolicy(maxAttempts, BackoffStrategy.EXPONENTIAL, initialDelay, maxDelay);
    }

    /**
     * Exponential backoff with defaults (2s initial, 5 min max)
     */
    public static RetryPolicy exponentialBackoff(int maxAttempts) {
        return exponentialBackoff(maxAttempts, Duration.ofSeconds(2), Duration.ofMinutes(5));
    }

    /**
     * Linear backoff retry
     */
    public static RetryPolicy linearBackoff(int maxAttempts, Duration initialDelay, Duration maxDelay) {
        return new RetryPolicy(maxAttempts, BackoffStrategy.LINEAR, initialDelay, maxDelay);
    }

    /**
     * Backoff strategy
     */
    public enum BackoffStrategy {
        FIXED,        // Same delay every time
        LINEAR,       // delay * attemptNumber
        EXPONENTIAL   // delay * 2^(attemptNumber-1)
    }

    @Override
    public String toString() {
        return String.format("RetryPolicy{attempts=%d, strategy=%s, initial=%dms, max=%dms}",
            maxAttempts, backoffStrategy, initialDelay.toMillis(), maxDelay.toMillis());
    }
}
