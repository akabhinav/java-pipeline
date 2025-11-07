package com.enterprise.pipeline.orchestrator.examples;

import com.enterprise.pipeline.orchestrator.Pipeline;
import com.enterprise.pipeline.orchestrator.core.*;
import com.enterprise.pipeline.orchestrator.executor.DAGExecutionResult;
import com.enterprise.pipeline.orchestrator.retry.RetryPolicy;
import com.enterprise.pipeline.orchestrator.sla.SLAMonitor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Simple ETL Pipeline Example
 *
 * Demonstrates a basic Extract-Transform-Load workflow:
 * - Ingest: Load data from source
 * - Transform: Clean and transform data
 * - Load: Write to destination
 *
 * @author Enterprise Pipeline Platform
 */
public class SimpleETLPipeline {

    public static void main(String[] args) {
        System.out.println("=".repeat(80));
        System.out.println("  Simple ETL Pipeline Example");
        System.out.println("=".repeat(80));
        System.out.println();

        // Create jobs
        Job ingestJob = new IngestJob();
        Job transformJob = new TransformJob();
        Job loadJob = new LoadJob();

        // Build and run pipeline
        DAGExecutionResult result = Pipeline.create("simple-etl")
            .addJob("ingest", ingestJob)
            .addJob("transform", transformJob)
                .dependsOn("ingest")
            .addJob("load", loadJob)
                .dependsOn("transform")
            .retry(3)
            .sla(Duration.ofMinutes(5))
            .run();

        // Print results
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  Execution Results");
        System.out.println("=".repeat(80));
        System.out.println("Overall Status: " + result.getOverallStatus());
        System.out.println("Total Duration: " + result.getDuration().toMillis() + " ms");
        System.out.println("Jobs Succeeded: " + result.getSuccessCount());
        System.out.println("Jobs Failed: " + result.getFailureCount());
        System.out.println();

        System.out.println("Job Details:");
        result.getJobResults().forEach((jobId, jobResult) ->
            System.out.printf("  %s: %s (%d ms)%n",
                jobId, jobResult.getStatus(), jobResult.getDuration().toMillis()));

        System.out.println("\n" + "=".repeat(80));
    }

    /**
     * Ingest Job - Extract data from source
     */
    static class IngestJob implements Job {
        @Override
        public String getId() { return "ingest"; }

        @Override
        public String getName() { return "Ingest Data"; }

        @Override
        public JobResult execute(JobContext context) throws Exception {
            System.out.println("[Ingest] Loading data from source...");
            Thread.sleep(1000);  // Simulate work

            Instant start = Instant.now();
            // Simulate data ingestion
            int recordCount = 10000;
            System.out.println("[Ingest] Loaded " + recordCount + " records");

            return JobResult.builder(getId())
                .status(JobStatus.SUCCESS)
                .startTime(start)
                .endTime(Instant.now())
                .output(Map.of("recordCount", recordCount))
                .build();
        }
    }

    /**
     * Transform Job - Clean and transform data
     */
    static class TransformJob implements Job {
        @Override
        public String getId() { return "transform"; }

        @Override
        public String getName() { return "Transform Data"; }

        @Override
        public JobResult execute(JobContext context) throws Exception {
            // Get input from previous job
            JobResult ingestResult = context.getPreviousResult("ingest")
                .orElseThrow(() -> new Exception("Ingest job result not found"));

            int inputRecords = (int) ingestResult.getOutput().get("recordCount");
            System.out.println("[Transform] Processing " + inputRecords + " records...");
            Thread.sleep(1500);  // Simulate work

            Instant start = Instant.now();
            // Simulate transformation
            int outputRecords = (int) (inputRecords * 0.95);  // 5% filtered out
            System.out.println("[Transform] Transformed to " + outputRecords + " records");

            return JobResult.builder(getId())
                .status(JobStatus.SUCCESS)
                .startTime(start)
                .endTime(Instant.now())
                .output(Map.of("recordCount", outputRecords))
                .build();
        }
    }

    /**
     * Load Job - Write to destination
     */
    static class LoadJob implements Job {
        @Override
        public String getId() { return "load"; }

        @Override
        public String getName() { return "Load Data"; }

        @Override
        public JobResult execute(JobContext context) throws Exception {
            // Get input from previous job
            JobResult transformResult = context.getPreviousResult("transform")
                .orElseThrow(() -> new Exception("Transform job result not found"));

            int recordCount = (int) transformResult.getOutput().get("recordCount");
            System.out.println("[Load] Writing " + recordCount + " records to destination...");
            Thread.sleep(1000);  // Simulate work

            Instant start = Instant.now();
            System.out.println("[Load] Successfully loaded " + recordCount + " records");

            return JobResult.builder(getId())
                .status(JobStatus.SUCCESS)
                .startTime(start)
                .endTime(Instant.now())
                .output(Map.of("recordCount", recordCount))
                .build();
        }
    }
}
