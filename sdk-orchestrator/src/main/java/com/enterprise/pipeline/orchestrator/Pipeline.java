package com.enterprise.pipeline.orchestrator;

import com.enterprise.pipeline.orchestrator.core.Job;
import com.enterprise.pipeline.orchestrator.dag.DAG;
import com.enterprise.pipeline.orchestrator.executor.DAGExecutionResult;
import com.enterprise.pipeline.orchestrator.executor.DAGExecutor;
import com.enterprise.pipeline.orchestrator.retry.RetryPolicy;
import com.enterprise.pipeline.orchestrator.scheduler.CronScheduler;
import com.enterprise.pipeline.orchestrator.sla.SLAMonitor;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Pipeline - High-Level Orchestration API
 *
 * Provides a fluent API for building and executing workflows.
 *
 * Example:
 * <pre>
 * Pipeline.create("daily-etl")
 *     .addJob("ingest", ingestJob)
 *     .addJob("transform", transformJob)
 *         .dependsOn("ingest")
 *     .addJob("load", loadJob)
 *         .dependsOn("transform")
 *     .schedule("0 0 * * *")  // Daily at midnight
 *     .retry(3)
 *     .sla(Duration.ofHours(2))
 *     .run();
 * </pre>
 *
 * @author Enterprise Pipeline Platform
 */
public class Pipeline {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final String name;
    private final Map<String, Job> jobs;
    private final Map<String, List<String>> dependencies;
    private final Map<String, Object> parameters;
    private String cronExpression;
    private RetryPolicy retryPolicy;
    private SLAMonitor slaMonitor;
    private int maxParallelism;

    private Pipeline(String name) {
        this.name = name;
        this.jobs = new LinkedHashMap<>();
        this.dependencies = new HashMap<>();
        this.parameters = new HashMap<>();
        this.retryPolicy = RetryPolicy.exponentialBackoff(3);
        this.slaMonitor = SLAMonitor.disabled();
        this.maxParallelism = Runtime.getRuntime().availableProcessors();
    }

    /**
     * Create a new pipeline
     */
    public static Pipeline create(String name) {
        return new Pipeline(name);
    }

    /**
     * Add a job to the pipeline
     */
    public Pipeline addJob(String jobId, Job job) {
        jobs.put(jobId, job);
        return this;
    }

    /**
     * Add dependency (this job depends on another)
     */
    public Pipeline dependsOn(String... jobIds) {
        if (jobs.isEmpty()) {
            throw new IllegalStateException("No job to add dependency to. Call addJob() first.");
        }

        // Get last added job
        String lastJobId = getLastJobId();
        dependencies.computeIfAbsent(lastJobId, k -> new ArrayList<>())
            .addAll(Arrays.asList(jobIds));

        return this;
    }

    /**
     * Add parameter
     */
    public Pipeline withParameter(String key, Object value) {
        parameters.put(key, value);
        return this;
    }

    /**
     * Add multiple parameters
     */
    public Pipeline withParameters(Map<String, Object> params) {
        parameters.putAll(params);
        return this;
    }

    /**
     * Set cron schedule
     */
    public Pipeline schedule(String cronExpression) {
        this.cronExpression = cronExpression;
        return this;
    }

    /**
     * Set retry policy
     */
    public Pipeline retry(int maxAttempts) {
        this.retryPolicy = RetryPolicy.exponentialBackoff(maxAttempts);
        return this;
    }

    /**
     * Set retry policy with custom configuration
     */
    public Pipeline retry(RetryPolicy policy) {
        this.retryPolicy = policy;
        return this;
    }

    /**
     * Set SLA threshold
     */
    public Pipeline sla(Duration threshold) {
        this.slaMonitor = SLAMonitor.builder()
            .threshold(threshold)
            .alertToLog()
            .build();
        return this;
    }

    /**
     * Set SLA monitor
     */
    public Pipeline sla(SLAMonitor monitor) {
        this.slaMonitor = monitor;
        return this;
    }

    /**
     * Set max parallelism
     */
    public Pipeline maxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
        return this;
    }

    /**
     * Build DAG
     */
    private DAG buildDAG() {
        DAG.Builder dagBuilder = new DAG.Builder(name);

        // Add all jobs
        for (Job job : jobs.values()) {
            dagBuilder.addJob(job);
        }

        // Add dependencies
        for (Map.Entry<String, List<String>> entry : dependencies.entrySet()) {
            String jobId = entry.getKey();
            for (String dependsOn : entry.getValue()) {
                dagBuilder.addDependency(jobId, dependsOn);
            }
        }

        return dagBuilder.build();
    }

    /**
     * Run pipeline once
     */
    public DAGExecutionResult run() {
        logger.info("Running pipeline: {}", name);

        DAG dag = buildDAG();
        DAGExecutor executor = new DAGExecutor(maxParallelism, retryPolicy, slaMonitor);

        try {
            return executor.execute(dag, parameters);
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Start scheduled execution
     */
    public CronScheduler startScheduled() throws SchedulerException {
        if (cronExpression == null) {
            throw new IllegalStateException("No schedule defined. Call schedule() first.");
        }

        logger.info("Starting scheduled pipeline: {} with cron: {}", name, cronExpression);

        DAG dag = buildDAG();
        DAGExecutor executor = new DAGExecutor(maxParallelism, retryPolicy, slaMonitor);
        CronScheduler scheduler = new CronScheduler(executor);

        scheduler.schedule(dag, cronExpression, parameters);
        scheduler.start();

        return scheduler;
    }

    /**
     * Get last added job ID
     */
    private String getLastJobId() {
        return jobs.keySet().stream()
            .reduce((first, second) -> second)
            .orElseThrow(() -> new IllegalStateException("No jobs in pipeline"));
    }

    /**
     * Get pipeline name
     */
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return String.format("Pipeline{name=%s, jobs=%d, scheduled=%s}",
            name, jobs.size(), cronExpression != null);
    }
}
