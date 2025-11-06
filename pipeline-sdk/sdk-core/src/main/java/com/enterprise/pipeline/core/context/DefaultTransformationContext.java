package com.enterprise.pipeline.core.context;

import com.enterprise.pipeline.api.TransformationContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of TransformationContext.
 * Thread-safe using ConcurrentHashMap for shared state.
 *
 * @author Enterprise Data Pipeline Team
 */
public class DefaultTransformationContext implements TransformationContext {

    private static final long serialVersionUID = 1L;

    private final transient SparkSession sparkSession;
    private final Map<String, Dataset<Row>> datasets;
    private final Map<String, Object> pipelineConfig;
    private final Map<String, Object> metadata;

    public DefaultTransformationContext(SparkSession sparkSession, Map<String, Object> pipelineConfig) {
        this.sparkSession = sparkSession;
        this.pipelineConfig = Collections.unmodifiableMap(pipelineConfig);
        this.datasets = new ConcurrentHashMap<>();
        this.metadata = new ConcurrentHashMap<>();
    }

    @Override
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    @Override
    public Optional<Dataset<Row>> getDataset(String name) {
        return Optional.ofNullable(datasets.get(name));
    }

    @Override
    public void registerDataset(String name, Dataset<Row> dataset) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Dataset name cannot be null or blank");
        }
        if (dataset == null) {
            throw new IllegalArgumentException("Dataset cannot be null");
        }
        datasets.put(name, dataset);
    }

    @Override
    public Map<String, Object> getPipelineConfig() {
        return pipelineConfig;
    }

    @Override
    public Optional<Object> getMetadata(String key) {
        return Optional.ofNullable(metadata.get(key));
    }

    @Override
    public void setMetadata(String key, Object value) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("Metadata key cannot be null or blank");
        }
        metadata.put(key, value);
    }
}
