# Enterprise Data Pipeline Platform

A simple, extensible, production-grade data pipeline platform built on Apache Spark 3.5 and Java 17.

## Design Philosophy

- **Simplicity First**: Easy to read, understand, and modify
- **Highly Extensible**: Plugin architecture for everything
- **Best Practices**: SOLID principles, clean code, design patterns
- **Future-Proof**: Easy to enhance without breaking changes

## Key Features

✅ **Dual Pipeline Definition**: Build pipelines with Java Fluent API or JSON configuration
✅ **40+ Built-in Transformations**: Selection, filtering, aggregation, joins, window functions, and more
✅ **Multiple Connectors**: File (CSV, JSON, Parquet, Avro, ORC) and JDBC support
✅ **Window Functions**: Row number, rank, dense rank, lag, lead, running totals
✅ **Specialized Banking Transformations**: Account validation, credit scoring, fraud detection, interest calculation
✅ **Plugin Architecture**: Easy to add custom transformations
✅ **Type-Safe**: Compile-time validation with Java
✅ **Configuration-Driven**: Dynamic pipelines without code changes
✅ **Production-Ready**: Built on Apache Spark 3.5 for petabyte-scale data
✅ **Enterprise Features**: Data quality, null handling, advanced joins, PCI compliance

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

## Built-in Transformations (40+)

### Category 1: Selection & Filtering (8 transformations)
- `select` - Select specific columns
- `drop` - Drop columns
- `filter` - Filter rows by SQL condition
- `where` - Alias for filter
- `distinct` - Remove duplicate rows
- `distinctBy` - Remove duplicates based on specific columns
- `limit` - Limit number of rows
- `sample` - Random sample with or without replacement

### Category 2: Column Operations (7 transformations)
- `withColumn` - Add/modify column with SQL expression
- `renameColumn` - Rename a column
- `cast` - Cast column to different data type
- `coalesce` - Reduce number of partitions
- `repartition` - Repartition dataset (optionally by columns)
- `orderBy` - Sort dataset by columns (ascending/descending)
- `explode` - Expand array/map column into multiple rows

### Category 3: Aggregation (2 transformations)
- `groupBy` - Group by columns and aggregate (sum, count, avg, min, max, first, last)
- `aggregate` - Generic aggregation without grouping (produces single row)

### Category 4: Join Operations (7 transformations)
- `innerJoin` - Inner join with another dataset
- `leftJoin` - Left outer join
- `rightJoin` - Right outer join
- `fullOuterJoin` - Full outer join
- `leftSemiJoin` - Left semi join (filter on matching keys)
- `leftAntiJoin` - Left anti join (filter on non-matching keys)
- `crossJoin` - Cartesian product

### Category 5: Set Operations (4 transformations)
- `union` - Union datasets by position
- `unionByName` - Union datasets by column names
- `intersect` - Return rows that appear in both datasets
- `except` - Return rows in left dataset but not in right

### Category 6: Window Operations (4 transformations) 🆕
- `window` - Apply window function (sum, avg, min, max, lag, lead, first, last)
- `rowNumber` - Add row number within partitions
- `rank` - Add rank within partitions (with gaps for ties)
- `denseRank` - Add dense rank within partitions (no gaps for ties)

### Category 7: Null Handling (2 transformations)
- `fillNa` - Fill null values with specified value
- `dropNa` - Drop rows with null values (any/all strategy)

### Category 8: Utility (1 transformation)
- `cache` - Cache dataset in memory for faster access

### Specialized Banking Transformations (5 transformations) 🆕
- `validateAccount` - Validate account numbers using business rules
- `maskCreditCard` - Mask credit card numbers for PCI compliance (show last 4 digits)
- `calculateInterest` - Calculate simple or compound interest for loans/deposits
- `detectFraud` - Detect potentially fraudulent transactions based on rules
- `calculateCreditScore` - Calculate credit score based on banking behavior

## Data Connectors

### File Connector
**Formats:** CSV, JSON, Parquet, Avro, ORC

**Source Example:**
```json
{
  "type": "file",
  "config": {
    "path": "/data/input.csv",
    "format": "csv",
    "options": {
      "header": "true",
      "inferSchema": "true"
    }
  }
}
```

**Sink Example:**
```json
{
  "type": "file",
  "config": {
    "path": "/data/output.parquet",
    "format": "parquet",
    "mode": "overwrite",
    "partitionBy": ["year", "month"]
  }
}
```

### JDBC Connector
**Databases:** PostgreSQL, MySQL, Oracle, SQL Server, etc.

**Source Example:**
```json
{
  "type": "jdbc",
  "config": {
    "url": "jdbc:postgresql://localhost:5432/mydb",
    "dbtable": "customers",
    "user": "admin",
    "password": "secret",
    "numPartitions": 10,
    "partitionColumn": "customer_id",
    "lowerBound": "0",
    "upperBound": "1000000"
  }
}
```

**Sink Example:**
```json
{
  "type": "jdbc",
  "config": {
    "url": "jdbc:postgresql://localhost:5432/mydb",
    "dbtable": "processed_data",
    "user": "admin",
    "password": "secret",
    "mode": "append",
    "batchSize": "5000"
  }
}
```

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

### 1. Basic Pipeline Example
Simple pipeline demonstrating the Fluent Builder API.

```bash
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.BasicPipelineExample"
```

### 2. JSON Config Pipeline
Pipeline loaded from JSON configuration file.

```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.JsonConfigPipelineExample" \
  -Dexec.args="src/main/resources/sample-pipeline.json"
```

### 3. Banking Pipeline Example
Comprehensive example with multiple sources, joins, aggregations, and analytics.

```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.BankingPipelineExample"
```

**Features demonstrated:**
- Multiple data sources (customers, transactions)
- Complex joins (inner join with customer data)
- Data quality checks (null handling, validation)
- Risk scoring and categorization
- Aggregations by region and risk category
- Multiple output datasets

### 4. Custom Transformation Example
Shows how to create and register custom transformations.

```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.CustomTransformationExample"
```

**Features demonstrated:**
- Creating custom transformation (sensitive data masking)
- Registering with TransformationRegistry
- Using custom transformation in pipeline
- Example: Masking credit card and SSN numbers

### 5. Advanced Banking Pipeline Example 🆕
Comprehensive Phase 2 example with specialized banking transformations.

```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.AdvancedBankingPipelineExample"
```

**Features demonstrated:**
- **Window functions**: Row numbering, ranking, running totals
- **Specialized banking transformations**:
  - Account number validation
  - Credit card masking (PCI compliance)
  - Credit score calculation
  - Fraud detection
  - Interest calculation (simple & compound)
- **Complex multi-pipeline workflow**:
  - Customer account processing
  - Transaction fraud detection
  - Loan application processing
  - Customer rankings
- **Advanced analytics**: Customer segmentation, risk scoring, loan approvals

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

### Phase 1 (✅ Completed)
- ✅ Core SDK with 30+ foundational transformations
- ✅ File source/sink (CSV, JSON, Parquet, Avro, ORC)
- ✅ JDBC source/sink (PostgreSQL, MySQL, Oracle, SQL Server)
- ✅ JSON/YAML configuration
- ✅ Fluent Builder API
- ✅ Comprehensive examples (Basic, JSON, Banking, Custom)
- ✅ Spring Framework 6.1.x integration
- ✅ Plugin architecture with ServiceLoader SPI

### Phase 2 (✅ Completed)
- ✅ Window functions (Window, RowNumber, Rank, DenseRank)
- ✅ Additional aggregation functions (Aggregate)
- ✅ Specialized banking transformations (5+):
  - ✅ Account validation
  - ✅ Credit card masking (PCI compliance)
  - ✅ Interest calculation (simple & compound)
  - ✅ Fraud detection
  - ✅ Credit score calculation
- ✅ Advanced banking pipeline example
- ✅ 40+ total transformations

### Phase 3 (Next)
- ⏳ S3 connector (with AWS SDK integration)
- ⏳ Kafka connector (source & sink)
- ⏳ Additional banking transformations (15+ more):
  - Currency conversion
  - Risk assessment
  - KYC validation
  - Transaction categorization
- ⏳ Advanced transformations:
  - Pivot/Unpivot
  - Flatten nested structures
  - Custom UDFs support

### Phase 4 (Future)
- REST API for pipeline management
- Job scheduling with cron expressions
- Monitoring & metrics (Prometheus integration)
- Web UI for pipeline builder
- Data lineage tracking
- Pipeline versioning and rollback
- Real-time streaming support

## Support

For issues, questions, or contributions, please contact the Enterprise Data Pipeline Team.
