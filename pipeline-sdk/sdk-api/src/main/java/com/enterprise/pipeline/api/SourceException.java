package com.enterprise.pipeline.api;

/**
 * Exception thrown when a source read operation fails.
 *
 * @author Enterprise Data Pipeline Team
 */
public class SourceException extends PipelineException {

    private final String sourceName;

    public SourceException(String sourceName, String message) {
        super("Source '" + sourceName + "' failed: " + message);
        this.sourceName = sourceName;
    }

    public SourceException(String sourceName, String message, Throwable cause) {
        super("Source '" + sourceName + "' failed: " + message, cause);
        this.sourceName = sourceName;
    }

    public String getSourceName() {
        return sourceName;
    }
}
