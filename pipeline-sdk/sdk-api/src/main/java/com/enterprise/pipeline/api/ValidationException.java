package com.enterprise.pipeline.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Exception thrown when configuration validation fails.
 *
 * @author Enterprise Data Pipeline Team
 */
public class ValidationException extends PipelineException {

    private final List<String> validationErrors;

    public ValidationException(String message) {
        super(message);
        this.validationErrors = Collections.singletonList(message);
    }

    public ValidationException(List<String> validationErrors) {
        super("Validation failed: " + String.join(", ", validationErrors));
        this.validationErrors = new ArrayList<>(validationErrors);
    }

    public List<String> getValidationErrors() {
        return Collections.unmodifiableList(validationErrors);
    }
}
