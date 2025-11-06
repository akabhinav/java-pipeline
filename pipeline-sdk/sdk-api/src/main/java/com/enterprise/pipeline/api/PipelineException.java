package com.enterprise.pipeline.api;

/**
 * Base exception for all pipeline-related errors.
 *
 * @author Enterprise Data Pipeline Team
 */
public class PipelineException extends Exception {

    public PipelineException(String message) {
        super(message);
    }

    public PipelineException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineException(Throwable cause) {
        super(cause);
    }
}
