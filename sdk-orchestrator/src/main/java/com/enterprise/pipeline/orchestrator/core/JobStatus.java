package com.enterprise.pipeline.orchestrator.core;

/**
 * Job Execution Status
 *
 * @author Enterprise Pipeline Platform
 */
public enum JobStatus {
    PENDING,      // Job is waiting to execute
    RUNNING,      // Job is currently executing
    SUCCESS,      // Job completed successfully
    FAILED,       // Job failed
    SKIPPED,      // Job was skipped (conditional execution)
    CANCELLED,    // Job was cancelled
    TIMEOUT       // Job exceeded timeout
}
