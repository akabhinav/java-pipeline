package com.enterprise.pipeline.orchestrator.core;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Job Execution Context
 *
 * Provides access to:
 * - Job parameters
 * - Previous job outputs
 * - Execution metadata
 * - Shared state
 *
 * @author Enterprise Pipeline Platform
 */
public class JobContext {

    private final String executionId;
    private final Instant executionTime;
    private final Map<String, Object> parameters;
    private final Map<String, JobResult> previousResults;
    private final Map<String, Object> sharedState;

    public JobContext(String executionId, Instant executionTime,
                     Map<String, Object> parameters) {
        this.executionId = executionId;
        this.executionTime = executionTime;
        this.parameters = new ConcurrentHashMap<>(parameters);
        this.previousResults = new ConcurrentHashMap<>();
        this.sharedState = new ConcurrentHashMap<>();
    }

    public String getExecutionId() {
        return executionId;
    }

    public Instant getExecutionTime() {
        return executionTime;
    }

    /**
     * Get job parameter
     */
    public <T> Optional<T> getParameter(String key) {
        return Optional.ofNullable((T) parameters.get(key));
    }

    /**
     * Get all parameters
     */
    public Map<String, Object> getParameters() {
        return Map.copyOf(parameters);
    }

    /**
     * Get result from previous job
     */
    public Optional<JobResult> getPreviousResult(String jobId) {
        return Optional.ofNullable(previousResults.get(jobId));
    }

    /**
     * Store result from completed job
     */
    public void storePreviousResult(String jobId, JobResult result) {
        previousResults.put(jobId, result);
    }

    /**
     * Get shared state value
     */
    public <T> Optional<T> getSharedState(String key) {
        return Optional.ofNullable((T) sharedState.get(key));
    }

    /**
     * Set shared state value (for inter-job communication)
     */
    public void setSharedState(String key, Object value) {
        sharedState.put(key, value);
    }
}
