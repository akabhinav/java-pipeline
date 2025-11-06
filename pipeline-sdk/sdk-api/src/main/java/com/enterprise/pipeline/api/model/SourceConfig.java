package com.enterprise.pipeline.api.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Configuration for a data source.
 *
 * @author Enterprise Data Pipeline Team
 */
public record SourceConfig(
        String type,
        String name,
        Map<String, Object> config
) implements Serializable {

    public SourceConfig {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Source type cannot be null or blank");
        }
    }
}
