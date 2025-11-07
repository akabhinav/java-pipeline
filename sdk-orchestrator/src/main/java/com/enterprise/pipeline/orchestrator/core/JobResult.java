package com.enterprise.pipeline.orchestrator.core;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Job Execution Result
 *
 * Contains:
 * - Success/failure status
 * - Output data
 * - Execution metrics
 * - Error information (if failed)
 *
 * @author Enterprise Pipeline Platform
 */
public class JobResult {

    private final String jobId;
    private final JobStatus status;
    private final Instant startTime;
    private final Instant endTime;
    private final Duration duration;
    private final Map<String, Object> output;
    private final Throwable error;
    private final Map<String, Object> metrics;

    private JobResult(Builder builder) {
        this.jobId = builder.jobId;
        this.status = builder.status;
        this.startTime = builder.startTime;
        this.endTime = builder.endTime;
        this.duration = Duration.between(startTime, endTime);
        this.output = builder.output;
        this.error = builder.error;
        this.metrics = builder.metrics;
    }

    public String getJobId() { return jobId; }
    public JobStatus getStatus() { return status; }
    public Instant getStartTime() { return startTime; }
    public Instant getEndTime() { return endTime; }
    public Duration getDuration() { return duration; }
    public Map<String, Object> getOutput() { return output; }
    public Optional<Throwable> getError() { return Optional.ofNullable(error); }
    public Map<String, Object> getMetrics() { return metrics; }

    public boolean isSuccess() {
        return status == JobStatus.SUCCESS;
    }

    public boolean isFailure() {
        return status == JobStatus.FAILED;
    }

    public static Builder builder(String jobId) {
        return new Builder(jobId);
    }

    public static class Builder {
        private final String jobId;
        private JobStatus status;
        private Instant startTime;
        private Instant endTime;
        private Map<String, Object> output = Map.of();
        private Throwable error;
        private Map<String, Object> metrics = Map.of();

        public Builder(String jobId) {
            this.jobId = jobId;
        }

        public Builder status(JobStatus status) {
            this.status = status;
            return this;
        }

        public Builder startTime(Instant startTime) {
            this.startTime = startTime;
            return this;
        }

        public Builder endTime(Instant endTime) {
            this.endTime = endTime;
            return this;
        }

        public Builder output(Map<String, Object> output) {
            this.output = output;
            return this;
        }

        public Builder error(Throwable error) {
            this.error = error;
            return this;
        }

        public Builder metrics(Map<String, Object> metrics) {
            this.metrics = metrics;
            return this;
        }

        public JobResult build() {
            if (startTime == null) startTime = Instant.now();
            if (endTime == null) endTime = Instant.now();
            if (status == null) status = JobStatus.SUCCESS;
            return new JobResult(this);
        }
    }

    @Override
    public String toString() {
        return String.format("JobResult{job=%s, status=%s, duration=%dms}",
            jobId, status, duration.toMillis());
    }
}
