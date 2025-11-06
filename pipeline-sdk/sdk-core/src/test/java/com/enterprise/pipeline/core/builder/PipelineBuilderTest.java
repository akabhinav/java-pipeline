package com.enterprise.pipeline.core.builder;

import com.enterprise.pipeline.api.model.PipelineConfig;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for PipelineBuilder.
 *
 * @author Enterprise Data Pipeline Team
 */
class PipelineBuilderTest {

    @Test
    void shouldBuildBasicPipeline() {
        // Given & When
        PipelineConfig config = PipelineBuilder.create("test-pipeline")
                .fromSource("file", Map.of("path", "/data/input.csv"))
                .transform("select", Map.of("columns", List.of("id", "name")))
                .toSink("file", Map.of("path", "/data/output.parquet"))
                .build();

        // Then
        assertThat(config.name()).isEqualTo("test-pipeline");
        assertThat(config.version()).isEqualTo("1.0");
        assertThat(config.source()).isNotNull();
        assertThat(config.source().type()).isEqualTo("file");
        assertThat(config.transformations()).hasSize(1);
        assertThat(config.sink()).isNotNull();
        assertThat(config.sink().type()).isEqualTo("file");
    }

    @Test
    void shouldBuildPipelineWithMultipleTransformations() {
        // Given & When
        PipelineConfig config = PipelineBuilder.create("multi-transform-pipeline")
                .fromSource("file", Map.of("path", "/data/input.csv"))
                .transform("select", Map.of("columns", List.of("id", "name")))
                .transform("filter", Map.of("condition", "age > 18"))
                .transform("limit", Map.of("count", 100))
                .toSink("file", Map.of("path", "/data/output.parquet"))
                .build();

        // Then
        assertThat(config.transformations()).hasSize(3);
        assertThat(config.transformations().get(0).type()).isEqualTo("select");
        assertThat(config.transformations().get(1).type()).isEqualTo("filter");
        assertThat(config.transformations().get(2).type()).isEqualTo("limit");
    }

    @Test
    void shouldBuildPipelineWithSettings() {
        // Given & When
        PipelineConfig config = PipelineBuilder.create("settings-pipeline")
                .fromSource("file", Map.of("path", "/data/input.csv"))
                .toSink("file", Map.of("path", "/data/output.parquet"))
                .setting("spark.sql.shuffle.partitions", "200")
                .setting("pipeline.description", "Test pipeline")
                .build();

        // Then
        assertThat(config.settings()).hasSize(2);
        assertThat(config.getSetting("spark.sql.shuffle.partitions", "")).isEqualTo("200");
        assertThat(config.getSetting("pipeline.description", "")).isEqualTo("Test pipeline");
    }

    @Test
    void shouldThrowExceptionWhenSourceIsMissing() {
        // Given
        PipelineBuilder builder = PipelineBuilder.create("test-pipeline")
                .toSink("file", Map.of("path", "/data/output.parquet"));

        // When & Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source is required");
    }

    @Test
    void shouldThrowExceptionWhenSinkIsMissing() {
        // Given
        PipelineBuilder builder = PipelineBuilder.create("test-pipeline")
                .fromSource("file", Map.of("path", "/data/input.csv"));

        // When & Then
        assertThatThrownBy(builder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("sink is required");
    }

    @Test
    void shouldSupportNamedSourceAndTransformations() {
        // Given & When
        PipelineConfig config = PipelineBuilder.create("named-pipeline")
                .fromSource("file", "customers", Map.of("path", "/data/customers.csv"))
                .transform("select", "selected_customers", Map.of("columns", List.of("id", "name")))
                .toSink("file", Map.of("path", "/data/output.parquet"))
                .build();

        // Then
        assertThat(config.source().name()).isEqualTo("customers");
        assertThat(config.transformations().get(0).name()).isEqualTo("selected_customers");
    }
}
