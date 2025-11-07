package com.enterprise.pipeline.listeners;

import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Performance Metrics Listener
 *
 * Tracks job execution performance across ALL industries.
 *
 * Metrics Collected:
 * - Job execution time
 * - Stage execution time
 * - Task metrics (count, duration, failures)
 * - Shuffle read/write bytes
 * - Input/output records
 * - Memory usage
 *
 * Use Cases:
 * - SLA monitoring (job completes within X minutes)
 * - Performance regression detection
 * - Resource optimization
 * - Capacity planning
 *
 * @author Enterprise Pipeline Platform
 */
public class PerformanceMetricsListener extends SparkListener {

    private static final Logger logger = LoggerFactory.getLogger(PerformanceMetricsListener.class);

    // Track job start times
    private final Map<Integer, Long> jobStartTimes = new ConcurrentHashMap<>();

    // Track stage start times
    private final Map<Integer, Long> stageStartTimes = new ConcurrentHashMap<>();

    // Metrics storage
    private final Map<Integer, JobMetrics> jobMetrics = new ConcurrentHashMap<>();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        int jobId = jobStart.jobId();
        long startTime = jobStart.time();

        jobStartTimes.put(jobId, startTime);
        jobMetrics.put(jobId, new JobMetrics(jobId, startTime));

        logger.info("Job {} started at {}", jobId, startTime);
        logger.info("Job {} stages: {}", jobId, jobStart.stageIds().size());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        int jobId = jobEnd.jobId();
        long endTime = jobEnd.time();

        Long startTime = jobStartTimes.remove(jobId);
        if (startTime != null) {
            long duration = endTime - startTime;

            JobMetrics metrics = jobMetrics.get(jobId);
            if (metrics != null) {
                metrics.setDuration(duration);
                metrics.setSuccess(jobEnd.jobResult().toString().equals("JobSucceeded"));
            }

            logger.info("Job {} completed in {} ms", jobId, duration);
            logger.info("Job {} result: {}", jobId, jobEnd.jobResult());

            // Alert if job exceeds threshold (e.g., 5 minutes)
            if (duration > 300000) {
                logger.warn("Job {} exceeded 5-minute threshold: {} ms", jobId, duration);
            }
        }
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        StageInfo stageInfo = stageSubmitted.stageInfo();
        int stageId = stageInfo.stageId();

        stageStartTimes.put(stageId, stageInfo.submissionTime().getOrElse(() -> System.currentTimeMillis()));

        logger.info("Stage {} submitted: {} tasks", stageId, stageInfo.numTasks());
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        StageInfo stageInfo = stageCompleted.stageInfo();
        int stageId = stageInfo.stageId();

        Long startTime = stageStartTimes.remove(stageId);
        if (startTime != null && stageInfo.completionTime().isDefined()) {
            long duration = stageInfo.completionTime().get() - startTime;

            logger.info("Stage {} completed in {} ms", stageId, duration);
            logger.info("Stage {} metrics:", stageId);

            // Task metrics
            if (stageInfo.taskMetrics() != null) {
                logger.info("  - Executor CPU time: {} ms", stageInfo.taskMetrics().executorCpuTime() / 1000000);
                logger.info("  - Executor run time: {} ms", stageInfo.taskMetrics().executorRunTime());
                logger.info("  - Result size: {} bytes", stageInfo.taskMetrics().resultSize());
                logger.info("  - JVM GC time: {} ms", stageInfo.taskMetrics().jvmGCTime());
                logger.info("  - Memory bytes spilled: {}", stageInfo.taskMetrics().memoryBytesSpilled());
                logger.info("  - Disk bytes spilled: {}", stageInfo.taskMetrics().diskBytesSpilled());

                // Shuffle metrics
                if (stageInfo.taskMetrics().shuffleReadMetrics() != null) {
                    logger.info("  - Shuffle read bytes: {}",
                        stageInfo.taskMetrics().shuffleReadMetrics().totalBytesRead());
                    logger.info("  - Shuffle read records: {}",
                        stageInfo.taskMetrics().shuffleReadMetrics().recordsRead());
                }

                if (stageInfo.taskMetrics().shuffleWriteMetrics() != null) {
                    logger.info("  - Shuffle write bytes: {}",
                        stageInfo.taskMetrics().shuffleWriteMetrics().bytesWritten());
                    logger.info("  - Shuffle write records: {}",
                        stageInfo.taskMetrics().shuffleWriteMetrics().recordsWritten());
                }

                // Input metrics
                if (stageInfo.taskMetrics().inputMetrics() != null) {
                    logger.info("  - Input bytes read: {}",
                        stageInfo.taskMetrics().inputMetrics().bytesRead());
                    logger.info("  - Input records read: {}",
                        stageInfo.taskMetrics().inputMetrics().recordsRead());
                }

                // Output metrics
                if (stageInfo.taskMetrics().outputMetrics() != null) {
                    logger.info("  - Output bytes written: {}",
                        stageInfo.taskMetrics().outputMetrics().bytesWritten());
                    logger.info("  - Output records written: {}",
                        stageInfo.taskMetrics().outputMetrics().recordsWritten());
                }
            }
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        if (taskEnd.reason() instanceof org.apache.spark.Success$) {
            // Task succeeded
            if (taskEnd.taskMetrics() != null) {
                long executorTime = taskEnd.taskMetrics().executorRunTime();

                // Alert on slow tasks (> 1 minute)
                if (executorTime > 60000) {
                    logger.warn("Slow task detected: Stage {}, Task {}, Duration: {} ms",
                        taskEnd.stageId(), taskEnd.taskInfo().taskId(), executorTime);
                }
            }
        } else {
            // Task failed
            logger.error("Task failed: Stage {}, Task {}, Reason: {}",
                taskEnd.stageId(), taskEnd.taskInfo().taskId(), taskEnd.reason());
        }
    }

    /**
     * Get metrics for a specific job
     */
    public JobMetrics getJobMetrics(int jobId) {
        return jobMetrics.get(jobId);
    }

    /**
     * Get all job metrics
     */
    public Map<Integer, JobMetrics> getAllJobMetrics() {
        return new ConcurrentHashMap<>(jobMetrics);
    }

    /**
     * Job Metrics POJO
     */
    public static class JobMetrics {
        private final int jobId;
        private final long startTime;
        private long duration;
        private boolean success;

        public JobMetrics(int jobId, long startTime) {
            this.jobId = jobId;
            this.startTime = startTime;
        }

        public void setDuration(long duration) {
            this.duration = duration;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public int getJobId() { return jobId; }
        public long getStartTime() { return startTime; }
        public long getDuration() { return duration; }
        public boolean isSuccess() { return success; }

        @Override
        public String toString() {
            return String.format("JobMetrics{jobId=%d, duration=%dms, success=%s}",
                jobId, duration, success);
        }
    }
}
