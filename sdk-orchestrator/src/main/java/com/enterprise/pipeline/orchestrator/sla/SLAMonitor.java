package com.enterprise.pipeline.orchestrator.sla;

import com.enterprise.pipeline.orchestrator.core.Job;
import com.enterprise.pipeline.orchestrator.core.JobResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * SLA Monitor
 *
 * Monitors job execution against SLA thresholds and triggers alerts
 *
 * @author Enterprise Pipeline Platform
 */
public class SLAMonitor {

    private static final Logger logger = LoggerFactory.getLogger(SLAMonitor.class);

    private final Duration slaThreshold;
    private final List<Consumer<SLAViolation>> alertHandlers;
    private final boolean enabled;

    private SLAMonitor(Duration slaThreshold, List<Consumer<SLAViolation>> alertHandlers, boolean enabled) {
        this.slaThreshold = slaThreshold;
        this.alertHandlers = new ArrayList<>(alertHandlers);
        this.enabled = enabled;
    }

    /**
     * Check if job violated SLA
     */
    public void checkSLA(Job job, JobResult result) {
        if (!enabled) {
            return;
        }

        // Use job-specific timeout if available, otherwise use global SLA
        Duration threshold = job.getTimeout() != null ? job.getTimeout() : slaThreshold;

        if (result.getDuration().compareTo(threshold) > 0) {
            // SLA violated
            SLAViolation violation = new SLAViolation(
                job.getId(),
                job.getName(),
                threshold,
                result.getDuration(),
                result
            );

            logger.warn("SLA VIOLATION: Job {} exceeded threshold of {}ms (actual: {}ms)",
                job.getId(), threshold.toMillis(), result.getDuration().toMillis());

            // Trigger alerts
            for (Consumer<SLAViolation> handler : alertHandlers) {
                try {
                    handler.accept(violation);
                } catch (Exception e) {
                    logger.error("Error in SLA alert handler", e);
                }
            }
        }
    }

    /**
     * Disabled SLA monitor
     */
    public static SLAMonitor disabled() {
        return new SLAMonitor(Duration.ofDays(365), List.of(), false);
    }

    /**
     * SLA Monitor builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * SLA Violation
     */
    public static class SLAViolation {
        private final String jobId;
        private final String jobName;
        private final Duration expectedDuration;
        private final Duration actualDuration;
        private final JobResult result;

        public SLAViolation(String jobId, String jobName, Duration expectedDuration,
                           Duration actualDuration, JobResult result) {
            this.jobId = jobId;
            this.jobName = jobName;
            this.expectedDuration = expectedDuration;
            this.actualDuration = actualDuration;
            this.result = result;
        }

        public String getJobId() { return jobId; }
        public String getJobName() { return jobName; }
        public Duration getExpectedDuration() { return expectedDuration; }
        public Duration getActualDuration() { return actualDuration; }
        public JobResult getResult() { return result; }

        public Duration getOverage() {
            return actualDuration.minus(expectedDuration);
        }

        @Override
        public String toString() {
            return String.format("SLAViolation{job=%s, expected=%dms, actual=%dms, overage=%dms}",
                jobId, expectedDuration.toMillis(), actualDuration.toMillis(), getOverage().toMillis());
        }
    }

    /**
     * Builder
     */
    public static class Builder {
        private Duration slaThreshold = Duration.ofHours(1);
        private final List<Consumer<SLAViolation>> alertHandlers = new ArrayList<>();

        public Builder threshold(Duration threshold) {
            this.slaThreshold = threshold;
            return this;
        }

        public Builder onViolation(Consumer<SLAViolation> handler) {
            this.alertHandlers.add(handler);
            return this;
        }

        public Builder alertToLog() {
            return onViolation(violation ->
                logger.error("SLA ALERT: {}", violation));
            return this;
        }

        public Builder alertToSlack(String webhookUrl) {
            return onViolation(violation -> {
                // TODO: Implement Slack webhook
                logger.info("Would send Slack alert: {}", violation);
            });
        }

        public Builder alertToEmail(String emailAddress) {
            return onViolation(violation -> {
                // TODO: Implement email alert
                logger.info("Would send email to {}: {}", emailAddress, violation);
            });
        }

        public SLAMonitor build() {
            return new SLAMonitor(slaThreshold, alertHandlers, true);
        }
    }
}
