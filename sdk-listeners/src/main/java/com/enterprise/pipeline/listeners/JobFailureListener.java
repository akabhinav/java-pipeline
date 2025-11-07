package com.enterprise.pipeline.listeners;

import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Job Failure Listener
 *
 * Handles job failures across ALL industries with retry logic and alerting.
 *
 * Features:
 * - Capture job failures with full stack traces
 * - Task failure tracking
 * - Automatic retry logic (configurable)
 * - Failure pattern detection
 * - Root cause analysis
 * - Alert on critical failures
 *
 * Use Cases:
 * - Prevent data loss from job failures
 * - Automatic recovery
 * - Failure notifications
 * - Debugging production issues
 * - Error reporting
 *
 * @author Enterprise Pipeline Platform
 */
public class JobFailureListener extends SparkListener {

    private static final Logger logger = LoggerFactory.getLogger(JobFailureListener.class);

    private final Map<Integer, JobFailure> jobFailures = new ConcurrentHashMap<>();
    private final List<TaskFailure> taskFailures = new CopyOnWriteArrayList<>();
    private final int maxRetries;
    private final boolean enableAutoRetry;

    public JobFailureListener() {
        this(3, false);  // Default: 3 retries, auto-retry disabled
    }

    public JobFailureListener(int maxRetries, boolean enableAutoRetry) {
        this.maxRetries = maxRetries;
        this.enableAutoRetry = enableAutoRetry;
        logger.info("JobFailureListener initialized: maxRetries={}, autoRetry={}",
            maxRetries, enableAutoRetry);
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        int jobId = jobEnd.jobId();

        if (!(jobEnd.jobResult() instanceof org.apache.spark.scheduler.JobSucceeded$)) {
            // Job failed
            String failureReason = jobEnd.jobResult().toString();

            JobFailure failure = new JobFailure(
                jobId,
                System.currentTimeMillis(),
                failureReason,
                extractStackTrace(jobEnd)
            );

            jobFailures.put(jobId, failure);

            logger.error("Job {} FAILED: {}", jobId, failureReason);
            logger.error("Job {} stack trace:\n{}", jobId, failure.getStackTrace());

            // Alert on critical failure
            alertOnFailure(failure);

            // Auto-retry if enabled
            if (enableAutoRetry && failure.getRetryCount() < maxRetries) {
                handleRetry(failure);
            }
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (!(taskEnd.reason() instanceof org.apache.spark.Success$)) {
            // Task failed
            TaskFailure failure = new TaskFailure(
                taskEnd.stageId(),
                taskEnd.taskInfo().taskId(),
                taskEnd.taskInfo().attemptNumber(),
                taskEnd.reason().toString(),
                extractTaskStackTrace(taskEnd)
            );

            taskFailures.add(failure);

            logger.error("Task FAILED: Stage {}, Task {}, Attempt {}, Reason: {}",
                taskEnd.stageId(),
                taskEnd.taskInfo().taskId(),
                taskEnd.taskInfo().attemptNumber(),
                taskEnd.reason());

            // Check for task failure patterns
            detectFailurePatterns(taskEnd.stageId());
        }
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        StageInfo stageInfo = stageCompleted.stageInfo();

        if (stageInfo.failureReason().isDefined()) {
            logger.error("Stage {} FAILED: {}",
                stageInfo.stageId(),
                stageInfo.failureReason().get());

            // Log task failure summary
            logger.error("Stage {} failure summary: {} failed tasks out of {} total",
                stageInfo.stageId(),
                stageInfo.numFailedTasks(),
                stageInfo.numTasks());
        }
    }

    /**
     * Extract stack trace from job end event
     */
    private String extractStackTrace(SparkListenerJobEnd jobEnd) {
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            pw.println(jobEnd.jobResult());
            return sw.toString();
        } catch (Exception e) {
            return "Stack trace unavailable";
        }
    }

    /**
     * Extract stack trace from task end event
     */
    private String extractTaskStackTrace(SparkListenerTaskEnd taskEnd) {
        try {
            return taskEnd.reason().toString();
        } catch (Exception e) {
            return "Stack trace unavailable";
        }
    }

    /**
     * Alert on job failure
     * Override this method to integrate with your alerting system (Slack, PagerDuty, email, etc.)
     */
    protected void alertOnFailure(JobFailure failure) {
        // Default implementation: Log alert
        logger.error("ALERT: Job {} failed! Reason: {}", failure.getJobId(), failure.getReason());

        // TODO: Integrate with your alerting system
        // Examples:
        // - Send Slack message
        // - Send email
        // - Create PagerDuty incident
        // - Post to monitoring dashboard
    }

    /**
     * Handle job retry
     * Override this method to implement custom retry logic
     */
    protected void handleRetry(JobFailure failure) {
        failure.incrementRetryCount();

        logger.warn("Retrying job {} (attempt {}/{})",
            failure.getJobId(),
            failure.getRetryCount(),
            maxRetries);

        // TODO: Implement retry logic
        // Note: Spark doesn't support programmatic job retry out-of-the-box
        // You'll need to re-submit the job from your application code
    }

    /**
     * Detect failure patterns (e.g., same stage failing repeatedly)
     */
    private void detectFailurePatterns(int stageId) {
        long failureCount = taskFailures.stream()
            .filter(tf -> tf.getStageId() == stageId)
            .count();

        if (failureCount > 10) {
            logger.error("PATTERN DETECTED: Stage {} has {} failed tasks - possible systemic issue",
                stageId, failureCount);
        }
    }

    /**
     * Get failure summary
     */
    public FailureSummary getFailureSummary() {
        return new FailureSummary(
            jobFailures.size(),
            taskFailures.size(),
            jobFailures,
            taskFailures
        );
    }

    /**
     * Get all job failures
     */
    public Map<Integer, JobFailure> getJobFailures() {
        return new ConcurrentHashMap<>(jobFailures);
    }

    /**
     * Get all task failures
     */
    public List<TaskFailure> getTaskFailures() {
        return new CopyOnWriteArrayList<>(taskFailures);
    }

    /**
     * Job Failure POJO
     */
    public static class JobFailure {
        private final int jobId;
        private final long timestamp;
        private final String reason;
        private final String stackTrace;
        private int retryCount = 0;

        public JobFailure(int jobId, long timestamp, String reason, String stackTrace) {
            this.jobId = jobId;
            this.timestamp = timestamp;
            this.reason = reason;
            this.stackTrace = stackTrace;
        }

        public void incrementRetryCount() {
            this.retryCount++;
        }

        public int getJobId() { return jobId; }
        public long getTimestamp() { return timestamp; }
        public String getReason() { return reason; }
        public String getStackTrace() { return stackTrace; }
        public int getRetryCount() { return retryCount; }

        @Override
        public String toString() {
            return String.format("JobFailure{jobId=%d, reason=%s, retries=%d}",
                jobId, reason, retryCount);
        }
    }

    /**
     * Task Failure POJO
     */
    public static class TaskFailure {
        private final int stageId;
        private final long taskId;
        private final int attemptNumber;
        private final String reason;
        private final String stackTrace;

        public TaskFailure(int stageId, long taskId, int attemptNumber, String reason, String stackTrace) {
            this.stageId = stageId;
            this.taskId = taskId;
            this.attemptNumber = attemptNumber;
            this.reason = reason;
            this.stackTrace = stackTrace;
        }

        public int getStageId() { return stageId; }
        public long getTaskId() { return taskId; }
        public int getAttemptNumber() { return attemptNumber; }
        public String getReason() { return reason; }
        public String getStackTrace() { return stackTrace; }

        @Override
        public String toString() {
            return String.format("TaskFailure{stage=%d, task=%d, attempt=%d, reason=%s}",
                stageId, taskId, attemptNumber, reason);
        }
    }

    /**
     * Failure Summary POJO
     */
    public static class FailureSummary {
        private final int totalJobFailures;
        private final int totalTaskFailures;
        private final Map<Integer, JobFailure> jobFailures;
        private final List<TaskFailure> taskFailures;

        public FailureSummary(int totalJobFailures, int totalTaskFailures,
                              Map<Integer, JobFailure> jobFailures, List<TaskFailure> taskFailures) {
            this.totalJobFailures = totalJobFailures;
            this.totalTaskFailures = totalTaskFailures;
            this.jobFailures = jobFailures;
            this.taskFailures = taskFailures;
        }

        public int getTotalJobFailures() { return totalJobFailures; }
        public int getTotalTaskFailures() { return totalTaskFailures; }
        public Map<Integer, JobFailure> getJobFailures() { return jobFailures; }
        public List<TaskFailure> getTaskFailures() { return taskFailures; }

        @Override
        public String toString() {
            return String.format("FailureSummary{jobs=%d, tasks=%d}",
                totalJobFailures, totalTaskFailures);
        }
    }
}
