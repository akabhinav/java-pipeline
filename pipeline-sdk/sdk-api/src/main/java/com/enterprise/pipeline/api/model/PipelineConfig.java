package com.enterprise.pipeline.api.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Root configuration for a pipeline.
 * Immutable configuration using Java records.
 *
 * Design Principles:
 * - Immutable: Thread-safe
 * - Simple: Clear structure
 * - Flexible: Map-based configs for extensibility
 *
 * @author Enterprise Data Pipeline Team
 */
public record PipelineConfig(
        String name,
        String version,
        SourceConfig source,
        List<TransformationConfig> transformations,
        SinkConfig sink,
        Map<String, Object> settings
) implements Serializable {

    public PipelineConfig {
        // Compact constructor for validation
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Pipeline name cannot be null or blank");
        }
        if (source == null) {
            throw new IllegalArgumentException("Pipeline source cannot be null");
        }
        if (transformations == null) {
            throw new IllegalArgumentException("Pipeline transformations cannot be null");
        }
        if (sink == null) {
            throw new IllegalArgumentException("Pipeline sink cannot be null");
        }
    }

    /**
     * Get a setting value.
     *
     * @param key Setting key
     * @param defaultValue Default value if not found
     * @param <T> Value type
     * @return Setting value or default
     */
    @SuppressWarnings("unchecked")
    public <T> T getSetting(String key, T defaultValue) {
        if (settings == null) {
            return defaultValue;
        }
        return (T) settings.getOrDefault(key, defaultValue);
    }
}
