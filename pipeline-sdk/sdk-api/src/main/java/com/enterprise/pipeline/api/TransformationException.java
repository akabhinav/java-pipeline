package com.enterprise.pipeline.api;

/**
 * Exception thrown when a transformation fails.
 *
 * @author Enterprise Data Pipeline Team
 */
public class TransformationException extends PipelineException {

    private final String transformationName;

    public TransformationException(String transformationName, String message) {
        super("Transformation '" + transformationName + "' failed: " + message);
        this.transformationName = transformationName;
    }

    public TransformationException(String transformationName, String message, Throwable cause) {
        super("Transformation '" + transformationName + "' failed: " + message, cause);
        this.transformationName = transformationName;
    }

    public String getTransformationName() {
        return transformationName;
    }
}
