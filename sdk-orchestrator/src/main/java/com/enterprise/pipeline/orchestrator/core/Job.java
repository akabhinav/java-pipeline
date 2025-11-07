package com.enterprise.pipeline.orchestrator.core;

import java.time.Duration;
import java.util.Map;

/**
 * Job Interface
 *
 * Represents a single executable unit in a pipeline workflow.
 * A job can be a Spark job, database query, API call, shell script, etc.
 *
 * @author Enterprise Pipeline Platform
 */
public interface Job {

    /**
     * Get unique job identifier
     */
    String getId();

    /**
     * Get human-readable job name
     */
    String getName();

    /**
     * Execute the job
     *
     * @param context Job execution context (parameters, previous job outputs, etc.)
     * @return Job execution result
     * @throws Exception if job execution fails
     */
    JobResult execute(JobContext context) throws Exception;

    /**
     * Get job timeout (optional)
     * Returns null if no timeout
     */
    default Duration getTimeout() {
        return null;
    }

    /**
     * Get job priority (higher = more important)
     * Default: 0 (normal priority)
     */
    default int getPriority() {
        return 0;
    }

    /**
     * Get job metadata (tags, owner, etc.)
     */
    default Map<String, String> getMetadata() {
        return Map.of();
    }

    /**
     * Check if job can be retried on failure
     */
    default boolean isRetryable() {
        return true;
    }
}
