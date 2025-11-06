package com.enterprise.pipeline.api;

/**
 * Exception thrown when a sink write operation fails.
 *
 * @author Enterprise Data Pipeline Team
 */
public class SinkException extends PipelineException {

    private final String sinkName;

    public SinkException(String sinkName, String message) {
        super("Sink '" + sinkName + "' failed: " + message);
        this.sinkName = sinkName;
    }

    public SinkException(String sinkName, String message, Throwable cause) {
        super("Sink '" + sinkName + "' failed: " + message, cause);
        this.sinkName = sinkName;
    }

    public String getSinkName() {
        return sinkName;
    }
}
