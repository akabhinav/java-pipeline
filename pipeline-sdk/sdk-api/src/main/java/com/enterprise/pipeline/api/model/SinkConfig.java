package com.enterprise.pipeline.api.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Configuration for a data sink.
 *
 * @author Enterprise Data Pipeline Team
 */
public record SinkConfig(
        String type,
        Map<String, Object> config
) implements Serializable {

    public SinkConfig {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("Sink type cannot be null or blank");
        }
    }
}
