package com.enterprise.pipeline.orchestrator.executor;

import com.enterprise.pipeline.orchestrator.core.JobResult;
import com.enterprise.pipeline.orchestrator.core.JobStatus;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * DAG Execution Result
 *
 * Contains results from executing a complete DAG workflow
 *
 * @author Enterprise Pipeline Platform
 */
public class DAGExecutionResult {

    private final String executionId;
    private final String dagName;
    private final Instant startTime;
    private final Instant endTime;
    private final Duration duration;
    private final Map<String, JobResult> jobResults;
    private final JobStatus overallStatus;

    public DAGExecutionResult(String executionId, String dagName, Instant startTime,
                             Instant endTime, Map<String, JobResult> jobResults,
                             JobStatus overallStatus) {
        this.executionId = executionId;
        this.dagName = dagName;
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = Duration.between(startTime, endTime);
        this.jobResults = Map.copyOf(jobResults);
        this.overallStatus = overallStatus;
    }

    public String getExecutionId() { return executionId; }
    public String getDagName() { return dagName; }
    public Instant getStartTime() { return startTime; }
    public Instant getEndTime() { return endTime; }
    public Duration getDuration() { return duration; }
    public Map<String, JobResult> getJobResults() { return jobResults; }
    public JobStatus getOverallStatus() { return overallStatus; }

    public boolean isSuccess() {
        return overallStatus == JobStatus.SUCCESS;
    }

    public boolean hasFailures() {
        return jobResults.values().stream().anyMatch(JobResult::isFailure);
    }

    public long getSuccessCount() {
        return jobResults.values().stream().filter(JobResult::isSuccess).count();
    }

    public long getFailureCount() {
        return jobResults.values().stream().filter(JobResult::isFailure).count();
    }

    @Override
    public String toString() {
        return String.format("DAGExecutionResult{dag=%s, status=%s, duration=%dms, " +
            "jobs=%d, success=%d, failed=%d}",
            dagName, overallStatus, duration.toMillis(),
            jobResults.size(), getSuccessCount(), getFailureCount());
    }
}
