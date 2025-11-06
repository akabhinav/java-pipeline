package com.enterprise.pipeline.core.builder;

import com.enterprise.pipeline.api.model.PipelineConfig;
import com.enterprise.pipeline.api.model.SinkConfig;
import com.enterprise.pipeline.api.model.SourceConfig;
import com.enterprise.pipeline.api.model.TransformationConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fluent builder for creating pipelines programmatically.
 *
 * Design Principles:
 * - Fluent API: Chainable methods
 * - Type-safe: Compile-time validation
 * - Simple: Easy to read and understand
 *
 * Example:
 * <pre>
 * PipelineConfig config = PipelineBuilder.create("my-pipeline")
 *     .fromSource("file", Map.of("path", "input.csv", "format", "csv"))
 *     .transform("select", Map.of("columns", List.of("id", "name")))
 *     .transform("filter", Map.of("condition", "age > 18"))
 *     .toSink("file", Map.of("path", "output.parquet", "format", "parquet"))
 *     .build();
 * </pre>
 *
 * @author Enterprise Data Pipeline Team
 */
public class PipelineBuilder {

    private String name;
    private String version = "1.0";
    private SourceConfig source;
    private final List<TransformationConfig> transformations = new ArrayList<>();
    private SinkConfig sink;
    private final Map<String, Object> settings = new HashMap<>();

    private PipelineBuilder(String name) {
        this.name = name;
    }

    /**
     * Create a new pipeline builder.
     *
     * @param name Pipeline name
     * @return New builder instance
     */
    public static PipelineBuilder create(String name) {
        return new PipelineBuilder(name);
    }

    /**
     * Set pipeline version.
     *
     * @param version Version string
     * @return This builder
     */
    public PipelineBuilder version(String version) {
        this.version = version;
        return this;
    }

    /**
     * Configure the data source.
     *
     * @param type Source type (e.g., "file", "jdbc", "kafka")
     * @param config Source configuration
     * @return This builder
     */
    public PipelineBuilder fromSource(String type, Map<String, Object> config) {
        this.source = new SourceConfig(type, null, config);
        return this;
    }

    /**
     * Configure the data source with a name.
     *
     * @param type Source type
     * @param name Dataset name for referencing
     * @param config Source configuration
     * @return This builder
     */
    public PipelineBuilder fromSource(String type, String name, Map<String, Object> config) {
        this.source = new SourceConfig(type, name, config);
        return this;
    }

    /**
     * Add a transformation step.
     *
     * @param type Transformation type (e.g., "select", "filter", "join")
     * @param config Transformation configuration
     * @return This builder
     */
    public PipelineBuilder transform(String type, Map<String, Object> config) {
        this.transformations.add(new TransformationConfig(type, null, config));
        return this;
    }

    /**
     * Add a named transformation step.
     *
     * @param type Transformation type
     * @param name Dataset name for referencing
     * @param config Transformation configuration
     * @return This builder
     */
    public PipelineBuilder transform(String type, String name, Map<String, Object> config) {
        this.transformations.add(new TransformationConfig(type, name, config));
        return this;
    }

    /**
     * Configure the data sink.
     *
     * @param type Sink type (e.g., "file", "jdbc", "kafka")
     * @param config Sink configuration
     * @return This builder
     */
    public PipelineBuilder toSink(String type, Map<String, Object> config) {
        this.sink = new SinkConfig(type, config);
        return this;
    }

    /**
     * Add a pipeline setting.
     *
     * @param key Setting key
     * @param value Setting value
     * @return This builder
     */
    public PipelineBuilder setting(String key, Object value) {
        this.settings.put(key, value);
        return this;
    }

    /**
     * Add multiple pipeline settings.
     *
     * @param settings Settings map
     * @return This builder
     */
    public PipelineBuilder settings(Map<String, Object> settings) {
        this.settings.putAll(settings);
        return this;
    }

    /**
     * Build the pipeline configuration.
     *
     * @return Immutable pipeline configuration
     * @throws IllegalStateException if required components are missing
     */
    public PipelineConfig build() {
        if (source == null) {
            throw new IllegalStateException("Pipeline source is required");
        }
        if (sink == null) {
            throw new IllegalStateException("Pipeline sink is required");
        }

        return new PipelineConfig(name, version, source, transformations, sink, settings);
    }
}
