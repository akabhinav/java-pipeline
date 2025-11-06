package com.enterprise.pipeline.api.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Configuration for a single transformation step.
 *
 * @author Enterprise Data Pipeline Team
 */
public record TransformationConfig(
        String type,
        String name,
        Map<String, Object> config
) implements Serializable {

    public TransformationConfig {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Transformation type cannot be null or blank");
        }
    }
}
