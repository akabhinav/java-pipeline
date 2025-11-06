package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SelectTransform.
 *
 * @author Enterprise Data Pipeline Team
 */
class SelectTransformTest {

    private static SparkSession spark;
    private final SelectTransform transform = new SelectTransform();

    @BeforeAll
    static void setupSpark() {
        spark = SparkSession.builder()
                .appName("SelectTransformTest")
                .master("local[1]")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void shouldReturnCorrectName() {
        assertThat(transform.getName()).isEqualTo("select");
    }

    @Test
    void shouldValidateRequiredColumns() {
        // Given
        Map<String, Object> emptyConfig = Map.of();

        // When & Then
        assertThatThrownBy(() -> transform.validate(emptyConfig))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("columns");
    }

    @Test
    void shouldValidateColumnsIsList() {
        // Given
        Map<String, Object> invalidConfig = Map.of("columns", "not a list");

        // When & Then
        assertThatThrownBy(() -> transform.validate(invalidConfig))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("must be a list");
    }

    @Test
    void shouldValidateColumnsNotEmpty() {
        // Given
        Map<String, Object> emptyListConfig = Map.of("columns", List.of());

        // When & Then
        assertThatThrownBy(() -> transform.validate(emptyListConfig))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("cannot be empty");
    }

    @Test
    void shouldSelectSpecifiedColumns() throws TransformationException {
        // Given
        Dataset<Row> input = spark.createDataFrame(
                List.of(
                        new TestData("1", "Alice", 25, "NY"),
                        new TestData("2", "Bob", 30, "LA")
                ),
                TestData.class
        );

        Map<String, Object> config = Map.of("columns", List.of("id", "name"));
        TransformationContext context = mock(TransformationContext.class);
        when(context.getSparkSession()).thenReturn(spark);

        // When
        Dataset<Row> result = transform.transform(input, config, context);

        // Then
        assertThat(result.columns()).containsExactly("id", "name");
        assertThat(result.count()).isEqualTo(2);
    }

    @Test
    void shouldThrowExceptionForNonExistentColumn() {
        // Given
        Dataset<Row> input = spark.createDataFrame(
                List.of(new TestData("1", "Alice", 25, "NY")),
                TestData.class
        );

        Map<String, Object> config = Map.of("columns", List.of("nonexistent"));
        TransformationContext context = mock(TransformationContext.class);
        when(context.getSparkSession()).thenReturn(spark);

        // When & Then
        assertThatThrownBy(() -> transform.transform(input, config, context))
                .isInstanceOf(TransformationException.class);
    }

    // Test data class
    public static class TestData {
        private String id;
        private String name;
        private int age;
        private String city;

        public TestData() {}

        public TestData(String id, String name, int age, String city) {
            this.id = id;
            this.name = name;
            this.age = age;
            this.city = city;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
        public String getCity() { return city; }
        public void setCity(String city) { this.city = city; }
    }
}
