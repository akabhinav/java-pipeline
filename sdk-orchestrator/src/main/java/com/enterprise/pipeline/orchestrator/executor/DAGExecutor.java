package com.enterprise.pipeline.orchestrator.executor;

import com.enterprise.pipeline.orchestrator.core.*;
import com.enterprise.pipeline.orchestrator.dag.DAG;
import com.enterprise.pipeline.orchestrator.retry.RetryPolicy;
import com.enterprise.pipeline.orchestrator.sla.SLAMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/**
 * DAG Executor
 *
 * Executes jobs in a DAG with:
 * - Parallel execution (jobs at same level run in parallel)
 * - Dependency management (wait for dependencies to complete)
 * - Retry on failure (configurable retry policy)
 * - Timeout handling
 * - SLA monitoring
 *
 * @author Enterprise Pipeline Platform
 */
public class DAGExecutor {

    private static final Logger logger = LoggerFactory.getLogger(DAGExecutor.class);

    private final ExecutorService executorService;
    private final RetryPolicy retryPolicy;
    private final SLAMonitor slaMonitor;
    private final int maxParallelism;

    public DAGExecutor(int maxParallelism, RetryPolicy retryPolicy, SLAMonitor slaMonitor) {
        this.maxParallelism = maxParallelism;
        this.executorService = Executors.newFixedThreadPool(maxParallelism);
        this.retryPolicy = retryPolicy != null ? retryPolicy : RetryPolicy.noRetry();
        this.slaMonitor = slaMonitor != null ? slaMonitor : SLAMonitor.disabled();
    }

    public DAGExecutor(int maxParallelism) {
        this(maxParallelism, null, null);
    }

    /**
     * Execute DAG
     *
     * Returns map of job ID -> job result
     */
    public DAGExecutionResult execute(DAG dag, Map<String, Object> parameters) {
        String executionId = UUID.randomUUID().toString();
        Instant startTime = Instant.now();

        logger.info("Starting DAG execution: {} (executionId={})", dag.getName(), executionId);
        logger.info("DAG has {} jobs to execute", dag.getJobCount());

        JobContext context = new JobContext(executionId, startTime, parameters);
        Map<String, JobResult> results = new ConcurrentHashMap<>();
        Map<String, Future<JobResult>> futures = new ConcurrentHashMap<>();

        try {
            // Get execution levels (jobs grouped by dependency level)
            List<Set<String>> levels = dag.getExecutionLevels();
            logger.info("DAG has {} execution levels", levels.size());

            // Execute each level
            for (int levelNum = 0; levelNum < levels.size(); levelNum++) {
                Set<String> level = levels.get(levelNum);
                logger.info("Executing level {}: {} jobs in parallel", levelNum, level.size());

                // Submit all jobs in this level
                for (String jobId : level) {
                    Job job = dag.getJob(jobId);

                    // Check if dependencies succeeded
                    if (!checkDependencies(dag, jobId, results)) {
                        logger.warn("Job {} skipped due to failed dependencies", jobId);
                        results.put(jobId, JobResult.builder(jobId)
                            .status(JobStatus.SKIPPED)
                            .startTime(Instant.now())
                            .endTime(Instant.now())
                            .build());
                        continue;
                    }

                    // Submit job for execution
                    Future<JobResult> future = executorService.submit(() ->
                        executeJob(job, context, executionId));
                    futures.put(jobId, future);
                }

                // Wait for all jobs in this level to complete
                for (String jobId : level) {
                    if (!futures.containsKey(jobId)) {
                        continue;  // Job was skipped
                    }

                    try {
                        JobResult result = futures.get(jobId).get();
                        results.put(jobId, result);
                        context.storePreviousResult(jobId, result);

                        logger.info("Job {} completed: {} in {}ms",
                            jobId, result.getStatus(), result.getDuration().toMillis());

                        // Check SLA
                        slaMonitor.checkSLA(dag.getJob(jobId), result);

                    } catch (ExecutionException e) {
                        logger.error("Job {} failed with exception", jobId, e.getCause());
                        JobResult failedResult = JobResult.builder(jobId)
                            .status(JobStatus.FAILED)
                            .startTime(Instant.now())
                            .endTime(Instant.now())
                            .error(e.getCause())
                            .build();
                        results.put(jobId, failedResult);
                        context.storePreviousResult(jobId, failedResult);
                    }
                }

                logger.info("Level {} completed", levelNum);
            }

            Instant endTime = Instant.now();
            Duration totalDuration = Duration.between(startTime, endTime);

            logger.info("DAG execution completed in {}ms", totalDuration.toMillis());

            return new DAGExecutionResult(
                executionId,
                dag.getName(),
                startTime,
                endTime,
                results,
                calculateOverallStatus(results)
            );

        } catch (Exception e) {
            logger.error("DAG execution failed", e);
            throw new RuntimeException("DAG execution failed", e);
        }
    }

    /**
     * Execute single job with retry logic
     */
    private JobResult executeJob(Job job, JobContext context, String executionId) {
        logger.info("Executing job: {} (executionId={})", job.getId(), executionId);

        Instant startTime = Instant.now();
        int attempt = 0;
        Throwable lastError = null;

        while (attempt < retryPolicy.getMaxAttempts()) {
            attempt++;

            try {
                // Execute with timeout if specified
                JobResult result;
                if (job.getTimeout() != null) {
                    result = executeWithTimeout(job, context, job.getTimeout());
                } else {
                    result = job.execute(context);
                }

                // Success
                if (result.isSuccess()) {
                    logger.info("Job {} succeeded on attempt {}", job.getId(), attempt);
                    return result;
                }

                lastError = result.getError().orElse(new Exception("Job failed"));

            } catch (Exception e) {
                lastError = e;
                logger.warn("Job {} failed on attempt {}: {}", job.getId(), attempt, e.getMessage());
            }

            // Retry if allowed
            if (attempt < retryPolicy.getMaxAttempts() && job.isRetryable()) {
                Duration delay = retryPolicy.getRetryDelay(attempt);
                logger.info("Retrying job {} in {}ms (attempt {}/{})",
                    job.getId(), delay.toMillis(), attempt + 1, retryPolicy.getMaxAttempts());

                try {
                    Thread.sleep(delay.toMillis());
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            } else {
                break;
            }
        }

        // All retries exhausted
        Instant endTime = Instant.now();
        logger.error("Job {} failed after {} attempts", job.getId(), attempt);

        return JobResult.builder(job.getId())
            .status(JobStatus.FAILED)
            .startTime(startTime)
            .endTime(endTime)
            .error(lastError)
            .build();
    }

    /**
     * Execute job with timeout
     */
    private JobResult executeWithTimeout(Job job, JobContext context, Duration timeout)
        throws Exception {
        Future<JobResult> future = executorService.submit(() -> job.execute(context));

        try {
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            logger.error("Job {} exceeded timeout of {}ms", job.getId(), timeout.toMillis());

            return JobResult.builder(job.getId())
                .status(JobStatus.TIMEOUT)
                .startTime(Instant.now())
                .endTime(Instant.now())
                .error(e)
                .build();
        }
    }

    /**
     * Check if all dependencies succeeded
     */
    private boolean checkDependencies(DAG dag, String jobId, Map<String, JobResult> results) {
        List<String> dependencies = dag.getDependencies(jobId);

        for (String dep : dependencies) {
            JobResult depResult = results.get(dep);
            if (depResult == null || !depResult.isSuccess()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Calculate overall status from all job results
     */
    private JobStatus calculateOverallStatus(Map<String, JobResult> results) {
        boolean anyFailed = results.values().stream().anyMatch(JobResult::isFailure);
        boolean allSuccess = results.values().stream().allMatch(JobResult::isSuccess);

        if (anyFailed) {
            return JobStatus.FAILED;
        } else if (allSuccess) {
            return JobStatus.SUCCESS;
        } else {
            return JobStatus.SKIPPED;  // Some jobs were skipped
        }
    }

    /**
     * Shutdown executor
     */
    public void shutdown() {
        logger.info("Shutting down DAG executor");
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
