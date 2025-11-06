# Enterprise Data Pipeline Platform

A simple, extensible, production-grade data pipeline platform built on Apache Spark 3.5 and Java 17.

## Design Philosophy

- **Simplicity First**: Easy to read, understand, and modify
- **Highly Extensible**: Plugin architecture for everything
- **Best Practices**: SOLID principles, clean code, design patterns
- **Future-Proof**: Easy to enhance without breaking changes

## Key Features

✅ **Dual Pipeline Definition**: Build pipelines with Java Fluent API or JSON configuration
✅ **14+ Built-in Transformations**: Selection, filtering, aggregation, joins, and more
✅ **Plugin Architecture**: Easy to add custom transformations
✅ **Type-Safe**: Compile-time validation with Java
✅ **Configuration-Driven**: Dynamic pipelines without code changes
✅ **Production-Ready**: Built on Apache Spark 3.5 for petabyte-scale data

## Quick Start

### Prerequisites

- Java 17 or higher (LTS)
- Apache Maven 3.8+
- Apache Spark 3.5
- Spring Framework 6.1.x (included via Maven dependencies)

### Build the Project

```bash
mvn clean install
```

### Method 1: Java Fluent API

```java
import com.enterprise.pipeline.core.builder.PipelineBuilder;
import com.enterprise.pipeline.core.engine.PipelineEngine;
import org.apache.spark.sql.SparkSession;

// Create Spark session
SparkSession spark = SparkSession.builder()
    .appName("My Pipeline")
    .master("local[*]")
    .getOrCreate();

// Build pipeline
PipelineConfig config = PipelineBuilder.create("customer-pipeline")
    .fromSource("file", Map.of(
        "path", "/data/customers.csv",
        "format", "csv",
        "options", Map.of("header", "true", "inferSchema", "true")
    ))
    .transform("filter", Map.of(
        "condition", "age >= 18"
    ))
    .transform("select", Map.of(
        "columns", List.of("id", "name", "age", "city")
    ))
    .toSink("file", Map.of(
        "path", "/output/customers.parquet",
        "format", "parquet"
    ))
    .build();

// Execute pipeline
PipelineEngine engine = new PipelineEngine(spark);
engine.execute(config);
```

### Method 2: JSON Configuration

**pipeline.json:**
```json
{
  "name": "customer-pipeline",
  "version": "1.0",
  "source": {
    "type": "file",
    "config": {
      "path": "/data/customers.csv",
      "format": "csv",
      "options": {
        "header": "true",
        "inferSchema": "true"
      }
    }
  },
  "transformations": [
    {
      "type": "filter",
      "config": {
        "condition": "age >= 18"
      }
    },
    {
      "type": "select",
      "config": {
        "columns": ["id", "name", "age", "city"]
      }
    }
  ],
  "sink": {
    "type": "file",
    "config": {
      "path": "/output/customers.parquet",
      "format": "parquet"
    }
  }
}
```

**Execute:**
```java
ConfigLoader loader = new ConfigLoader();
PipelineConfig config = loader.loadFromFile("pipeline.json");

PipelineEngine engine = new PipelineEngine(spark);
engine.execute(config);
```

## Built-in Transformations

### Category 1: Selection & Filtering
- `select` - Select specific columns
- `drop` - Drop columns
- `filter` / `where` - Filter rows by condition
- `distinct` - Remove duplicates
- `limit` - Limit number of rows
- `sample` - Random sample

### Category 2: Column Operations
- `withColumn` - Add/modify column with expression
- `renameColumn` - Rename a column

### Category 3: Aggregation
- `groupBy` - Group by columns and aggregate

### Category 4: Join Operations
- `innerJoin` - Inner join with another dataset

### Category 5: Set Operations
- `union` - Union multiple datasets

### Category 6: Null Handling
- `fillNa` - Fill null values
- `dropNa` - Drop rows with nulls

### Category 7: Utility
- `cache` - Cache dataset for performance

## Project Structure

```
data-pipeline-platform/
├── pipeline-sdk/                      # Core SDK
│   ├── sdk-api/                       # Interfaces and contracts
│   │   ├── Transformation.java
│   │   ├── Source.java
│   │   ├── Sink.java
│   │   └── TransformationContext.java
│   │
│   ├── sdk-core/                      # Core implementation
│   │   ├── engine/
│   │   │   └── PipelineEngine.java
│   │   ├── registry/
│   │   │   └── TransformationRegistry.java
│   │   ├── config/
│   │   │   └── ConfigLoader.java
│   │   └── builder/
│   │       └── PipelineBuilder.java
│   │
│   └── sdk-transformations/           # Built-in transformations
│       ├── foundational/
│       │   ├── SelectTransform.java
│       │   ├── FilterTransform.java
│       │   └── ...
│       └── connector/
│           ├── FileSource.java
│           └── FileSink.java
│
└── pipeline-examples/                 # Working examples
    └── BasicPipelineExample.java
```

## Creating Custom Transformations

### 1. Implement the Interface

```java
package com.mycompany.transform;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class MyCustomTransform implements Transformation {

    @Override
    public String getName() {
        return "myCustomTransform";
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input,
                                   Map<String, Object> config,
                                   TransformationContext context) {
        // Your transformation logic here
        return input.filter("your_condition");
    }
}
```

### 2. Register the Transformation

Create file: `src/main/resources/META-INF/services/com.enterprise.pipeline.api.Transformation`

```
com.mycompany.transform.MyCustomTransform
```

### 3. Use Your Transformation

```java
.transform("myCustomTransform", Map.of(
    "configKey", "configValue"
))
```

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│              USER INTERFACE (Java/JSON)                   │
└────────────────────┬─────────────────────────────────────┘
                     │
┌────────────────────┴─────────────────────────────────────┐
│            PIPELINE ENGINE (Orchestrator)                 │
│    - Loads config                                         │
│    - Wires components                                     │
│    - Executes pipeline                                    │
└────────────────────┬─────────────────────────────────────┘
                     │
┌────────────────────┴─────────────────────────────────────┐
│      TRANSFORMATION REGISTRY (Plugin Discovery)           │
│    - ServiceLoader SPI                                    │
│    - Discovers transformations                            │
└───┬─────────────┬─────────────┬──────────────────────────┘
    │             │             │
┌───┴───┐   ┌─────┴─────┐   ┌──┴──────┐
│SOURCE │   │TRANSFORM  │   │  SINK   │
│PLUGIN │   │  PLUGIN   │   │ PLUGIN  │
└───────┘   └───────────┘   └─────────┘
```

## Design Decisions

### Why Interface-Based Design?
✅ Easy to extend: Just implement interface
✅ Easy to test: Mock interfaces
✅ Easy to understand: Single responsibility
✅ Easy to modify: Change implementation without affecting users

### Why Java ServiceLoader (SPI)?
✅ No platform code changes needed
✅ Users can add transformations without touching core
✅ Follows Open/Closed Principle
✅ Industry standard pattern (JDBC drivers, SLF4J, etc.)

### Why JSON Configuration?
✅ Simple, human-readable
✅ Easy to generate programmatically
✅ Widely supported tooling
✅ Can be version controlled
✅ Easy to validate with JSON Schema

## Best Practices

### 1. Keep It Simple
- Write code that a junior developer can understand
- Avoid over-engineering and premature optimization
- Prefer composition over inheritance
- Use clear, descriptive names

### 2. Follow SOLID Principles
- **Single Responsibility**: Each class has one reason to change
- **Open/Closed**: Open for extension, closed for modification
- **Liskov Substitution**: Subtypes must be substitutable
- **Interface Segregation**: Many specific interfaces > one general
- **Dependency Inversion**: Depend on abstractions, not concretions

### 3. Error Handling
- Validate early, fail fast
- Provide clear error messages
- Use custom exceptions for different failure types

## Running Examples

### Basic Pipeline Example
```bash
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.BasicPipelineExample"
```

### JSON Config Pipeline
```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.JsonConfigPipelineExample" \
  -Dexec.args="src/main/resources/sample-pipeline.json"
```

## Technology Stack

- **Language**: Java 17 (LTS)
- **Processing**: Apache Spark 3.5
- **Framework**: Spring Framework 6.1.x (for DI and IoC)
- **Build**: Maven 3.8+
- **Testing**: JUnit 5, AssertJ, Mockito
- **Logging**: SLF4J + Logback
- **JSON**: Jackson
- **Annotations**: Jakarta Annotations 2.1

### Compatibility Matrix

| Component | Version | Notes |
|-----------|---------|-------|
| Java | 17+ | LTS version, required for Spring Framework 6.x |
| Spring Framework | 6.1.2 | Fully compatible with Java 17 and Spark 3.5 |
| Apache Spark | 3.5.0 | Scala 2.12 binary version |
| Jakarta Annotations | 2.1.1 | Required by Spring Framework 6.x |

**Why Spring Framework (not Spring Boot)?**
- Lightweight: Only essential DI/IoC features, no web server overhead
- Compatible: Works seamlessly with Spark 3.5's ClassLoader
- Flexible: Easy integration with Spark's execution model
- Simple: Minimal dependencies and configuration

## Contributing

1. Follow the design principles
2. Write tests for new transformations
3. Update documentation
4. Keep it simple!

## License

Copyright © 2024 Enterprise Data Pipeline Team

## Roadmap

### Phase 1 (Current)
- ✅ Core SDK with 14+ transformations
- ✅ File source/sink (CSV, JSON, Parquet)
- ✅ JSON configuration
- ✅ Fluent Builder API

### Phase 2 (Next)
- JDBC source/sink
- S3 connector
- Kafka connector
- 36 additional transformations

### Phase 3 (Future)
- REST API
- Job scheduling
- Monitoring & metrics
- Web UI

## Support

For issues, questions, or contributions, please contact the Enterprise Data Pipeline Team.
