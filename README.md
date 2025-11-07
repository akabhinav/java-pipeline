# Enterprise Data Pipeline Platform

A simple, extensible, production-grade data pipeline platform built on Apache Spark 3.5 and Java 17.

## Design Philosophy

- **Simplicity First**: Easy to read, understand, and modify
- **Highly Extensible**: Plugin architecture for everything
- **Best Practices**: SOLID principles, clean code, design patterns
- **Future-Proof**: Easy to enhance without breaking changes

## Key Features

✅ **Dual Pipeline Definition**: Build pipelines with Java Fluent API or JSON configuration
✅ **UI-Based Rule Engine**: Define validation, transformation, and business rules through visual UI
✅ **SDK Orchestrator**: DAG-based workflow orchestration for multi-job pipelines with retry, SLA, scheduling 🆕
✅ **50+ Built-in Transformations**: Selection, filtering, aggregation, joins, window functions, and more
✅ **Multiple Connectors**: File, JDBC, S3, and Kafka support
✅ **Cloud Storage**: AWS S3 connector with full SDK integration
✅ **Streaming Support**: Apache Kafka connector for real-time data processing
✅ **Data Lake (Delta Lake)**: Medallion architecture with ACID transactions, time travel, and schema evolution 🆕
✅ **SQL Query Engine (Trino)**: Query Data Lake with standard SQL, no Spark code needed 🆕
✅ **Catalog (Hive Metastore)**: Central metadata store for all tables, JDBC/ODBC connectivity 🆕
✅ **Window Functions**: Row number, rank, dense rank, lag, lead, running totals
✅ **Advanced Transformations**: Pivot, unpivot, flatten nested structures
✅ **Specialized Banking Transformations**: 9+ banking-specific transformations
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

## 🧪 Testing All Features - Comprehensive Pipeline

**Want to see everything in action?** We've created a complete end-to-end banking pipeline that demonstrates ALL platform features!

### Quick Test Run

```bash
# From project root
cd pipeline-examples

# Option 1: Automated script (recommended)
./run-comprehensive-pipeline.sh

# Option 2: Manual Maven execution
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.ComprehensiveBankingPipeline"
```

### What Gets Tested

The `ComprehensiveBankingPipeline` runs **5 complete scenarios**:

1. **📋 Loan Application Processing** (End-to-End)
   - ✅ Data validation with validation rules
   - ✅ Multi-source enrichment (customer data + credit bureau)
   - ✅ Credit scoring and grading
   - ✅ Business rule-based approval/rejection
   - ✅ EMI calculation for approved loans
   - ✅ Fraud detection
   - ✅ Risk-based filtering

2. **🔍 Real-Time Fraud Detection**
   - ✅ Transaction monitoring (simulating Kafka stream)
   - ✅ Multi-factor fraud scoring
   - ✅ High-value transaction filtering

3. **👤 Customer 360 View**
   - ✅ Multi-source data integration
   - ✅ Transaction aggregation
   - ✅ Customer lifetime value calculation

4. **⚖️ Regulatory Compliance**
   - ✅ KYC document validation
   - ✅ Compliance status checks
   - ✅ Rejection reason tracking

5. **📊 Advanced Analytics**
   - ✅ Risk-based interest rate determination
   - ✅ Revenue projection analysis
   - ✅ Portfolio analytics

### Expected Output

```
================================================================================
🚀 Comprehensive Banking Pipeline - Starting
================================================================================

📋 SCENARIO 1: End-to-End Loan Application Processing
  Step 1: Loading loan applications...
  ✓ Loaded 15 loan applications
  Step 2: Applying validation rules...
  ✓ Valid records: 15 / 15
  Step 3: Enriching with customer data from S3...
  ✓ Enriched with customer demographics
  ...

  📊 FINAL RESULTS:
  +---------------+---------------+------------+-------------+--------+
  | application_id| applicant_name| loan_amount| loan_status|   ...  |
  +---------------+---------------+------------+-------------+--------+
  | LA001         | John Doe      | 500000.0   | APPROVED    |   ...  |
  | LA002         | Jane Smith    | 150000.0   | REJECTED    |   ...  |
  +---------------+---------------+------------+-------------+--------+

  📈 PIPELINE STATISTICS:
  +-------------+-----+
  |  loan_status|count|
  +-------------+-----+
  |     APPROVED|   10|
  |     REJECTED|    5|
  +-------------+-----+

================================================================================
✅ All Pipeline Scenarios Completed Successfully!
================================================================================
```

### Features Demonstrated

| Feature Category | What's Tested | Code Location |
|-----------------|---------------|---------------|
| **Rule Engine** | All 4 rule types (Validation, Transformation, Business, Filter) | Lines 100-250 |
| **Transformations** | 50+ transformations across 9 categories | Lines 260-380 |
| **Connectors** | S3, Kafka, CSV, Parquet (simulated) | Lines 400-480 |
| **Banking Domain** | 9 banking-specific templates | Lines 500-650 |
| **Data Quality** | Validation, enrichment, cleansing | Lines 100-150 |
| **Real-time Processing** | Kafka stream simulation | Lines 300-350 |
| **Multi-source Integration** | 3+ source joins | Lines 400-450 |

### Customization

The comprehensive pipeline uses sample data. To test with your own data:

```java
// Edit ComprehensiveBankingPipeline.java
private static Dataset<Row> createLoanApplicationData(SparkSession spark) {
    // Replace with your data source
    return spark.read()
        .option("header", "true")
        .csv("s3://your-bucket/loan-applications.csv");
}
```

For complete documentation, see:
- [pipeline-examples/README.md](pipeline-examples/README.md) - Detailed guide
- [ComprehensiveBankingPipeline.java](pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java) - Full source code

---

## 🏗️ Data Lake on MinIO (Delta Lake) 🆕

Build a **production-grade Data Lake** on your local system using **MinIO** (S3-compatible) with **Delta Lake** format!

### What is it?

A complete **Medallion Architecture** data lake implementation:
- **Bronze Layer**: Raw, immutable data (append-only)
- **Silver Layer**: Cleaned, validated, deduplicated data
- **Gold Layer**: Business-ready aggregates and metrics

### Key Features

✅ **ACID Transactions** - Atomic writes, no partial updates
✅ **Time Travel** - Query any historical version of your data
✅ **Schema Evolution** - Add columns without breaking existing data
✅ **Upserts (MERGE)** - Update or insert in a single operation
✅ **Data Versioning** - Full audit trail of all changes
✅ **S3-Compatible** - Works with MinIO locally, AWS S3 in production

### Quick Start

```bash
# 1. Start local environment (includes MinIO)
./start-local-environment.sh   # Mac/Linux
start-local-environment.bat    # Windows

# 2. Run Data Lake pipeline
./run-data-lake.sh             # Mac/Linux
run-data-lake.bat              # Windows

# 3. View your Data Lake in MinIO Console
# http://localhost:9001 (minioadmin/minioadmin)
```

### What You Get

```
s3a://data-lake/
  ├── bronze/              # Raw data (append-only)
  │   ├── loan_applications/
  │   └── customers/
  ├── silver/              # Cleaned & validated
  │   ├── loan_applications_clean/
  │   └── customers_clean/
  └── gold/                # Business aggregates
      ├── loan_summary_by_purpose/
      └── customer_metrics/
```

### Example: Time Travel

```java
// Query current version
Dataset<Row> current = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean");

// Query yesterday's data
Dataset<Row> yesterday = spark.read()
    .format("delta")
    .option("timestampAsOf", "2025-11-05 10:00:00")
    .load("s3a://data-lake/silver/loan_applications_clean");

// View table history
DeltaTable deltaTable = DeltaTable.forPath(spark, path);
deltaTable.history(10).show();
```

### Example: Upsert (MERGE)

```java
// Update existing customers, insert new ones
DeltaTable silverTable = DeltaTable.forPath(spark, silverPath);

silverTable.as("target")
    .merge(updates.as("source"), "target.customer_id = source.customer_id")
    .whenMatched()
    .updateAll()      // Update if exists
    .whenNotMatched()
    .insertAll()      // Insert if new
    .execute();
```

### Complete Documentation

📖 **[DATA_LAKE_GUIDE.md](DATA_LAKE_GUIDE.md)** - Comprehensive guide covering:
- Medallion Architecture details
- All Delta Lake features with examples
- Time travel, upserts, schema evolution
- Performance optimization
- Production deployment
- Troubleshooting

📄 **[DataLakePipeline.java](pipeline-examples/src/main/java/com/enterprise/pipeline/examples/DataLakePipeline.java)** - Full source code

### Use Cases

1. **Data Warehousing**: Store all your business data with full history
2. **Regulatory Compliance**: Time travel for audit trails
3. **Data Quality**: Track quality metrics across layers
4. **Real-time Analytics**: Query current state with SQL
5. **Data Science**: Version control for ML training datasets

---

## 🔍 Catalog & SQL Query Engine (Trino) 🆕

Query your Data Lake with **standard SQL** - no Spark code needed! We've added **Hive Metastore** (catalog) and **Trino** (query engine) to your local environment.

### What You Get

**Hive Metastore** (Port 9083)
- Central metadata catalog for all Data Lake tables
- Stores schemas, partitions, locations
- PostgreSQL backend for persistence

**Trino Query Engine** (Port 8085)
- Fast distributed SQL query engine
- Query Delta Lake with standard SQL
- Web UI for monitoring: http://localhost:8085
- JDBC/ODBC for BI tools

### Quick Start

```bash
# 1. Start environment (includes Trino & Metastore)
./start-local-environment.sh

# 2. Create Data Lake
./run-data-lake.sh

# 3. Connect to Trino and query!
docker exec -it pipeline-trino trino --catalog delta --schema datalake
```

### Example Queries

```sql
-- Show all tables
SHOW TABLES;

-- Query Delta Lake table with SQL
SELECT * FROM loan_applications_silver LIMIT 10;

-- Aggregate query
SELECT
    loan_purpose,
    COUNT(*) as total_applications,
    SUM(loan_amount) as total_amount,
    AVG(loan_amount) as avg_amount
FROM loan_applications_silver
GROUP BY loan_purpose
ORDER BY total_amount DESC;

-- Join multiple tables
SELECT
    l.application_id,
    l.loan_amount,
    c.monthly_income,
    ROUND(l.loan_amount / c.monthly_income, 2) as loan_to_income_ratio
FROM loan_applications_silver l
JOIN customers_silver c ON l.customer_id = c.customer_id
ORDER BY loan_to_income_ratio DESC;
```

### Connect from BI Tools

**JDBC Connection String:**
```
jdbc:trino://localhost:8085/delta/datalake
```

**Supported Tools:**
- Tableau
- Power BI
- Apache Superset
- DBeaver
- Any JDBC-compatible tool

### Features

✅ **Standard SQL** - SELECT, JOIN, WHERE, GROUP BY, window functions
✅ **No Spark Code** - Pure SQL queries on Delta Lake
✅ **Web UI** - Visual query interface at http://localhost:8085
✅ **JDBC/ODBC** - Connect from any BI tool
✅ **Fast Execution** - Distributed query processing
✅ **70+ Query Examples** - Ready-to-use SQL queries

### Documentation

📖 **[CATALOG_GUIDE.md](CATALOG_GUIDE.md)** - Complete catalog & query guide covering:
- Architecture and setup
- SQL query examples (basic to advanced)
- JDBC/ODBC connection
- BI tool integration (Tableau, Power BI)
- Performance tips
- Troubleshooting

📄 **[docker/query-examples.sql](docker/query-examples.sql)** - 70+ ready-to-use SQL queries

### Alternative Metastores

We use **Hive Metastore** (production-standard), but you can also use:
- **Nessie** - Git-like versioning for tables
- **Unity Catalog** - Databricks governance
- **Polaris** - Snowflake Iceberg catalog
- **PostgreSQL Direct** - Simple, no extra service

See [CATALOG_GUIDE.md](CATALOG_GUIDE.md) for alternatives.

---

## Built-in Transformations (50+)

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

### Category 9: Advanced Transformations (3 transformations) 🆕
- `pivot` - Convert rows to columns (long to wide format)
- `unpivot` - Convert columns to rows (wide to long format)
- `flatten` - Flatten nested structures (struct, nested JSON)

### Specialized Banking Transformations (9 transformations) 🆕
- `validateAccount` - Validate account numbers using business rules
- `maskCreditCard` - Mask credit card numbers for PCI compliance (show last 4 digits)
- `calculateInterest` - Calculate simple or compound interest for loans/deposits
- `detectFraud` - Detect potentially fraudulent transactions based on rules
- `calculateCreditScore` - Calculate credit score based on banking behavior
- `convertCurrency` - Convert amounts between currencies with exchange rates
- `assessRisk` - Comprehensive risk assessment based on multiple factors
- `validateKyc` - Validate Know Your Customer (KYC) compliance
- `categorizeTransaction` - Categorize transactions into spending categories

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

### S3 Connector 🆕
**Cloud Storage:** AWS S3 buckets with full SDK integration

**Source Example:**
```json
{
  "type": "s3",
  "config": {
    "bucket": "my-data-bucket",
    "key": "data/customers.csv",
    "format": "csv",
    "region": "us-west-2",
    "accessKey": "${AWS_ACCESS_KEY}",
    "secretKey": "${AWS_SECRET_KEY}",
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
  "type": "s3",
  "config": {
    "bucket": "my-data-bucket",
    "key": "output/processed-data.parquet",
    "format": "parquet",
    "mode": "overwrite",
    "region": "us-west-2",
    "partitionBy": ["year", "month"]
  }
}
```

### Kafka Connector 🆕
**Streaming Platform:** Apache Kafka for real-time data processing

**Source Example:**
```json
{
  "type": "kafka",
  "config": {
    "bootstrapServers": "localhost:9092",
    "topic": "transactions",
    "startingOffsets": "earliest",
    "groupId": "pipeline-consumer-group"
  }
}
```

**Sink Example:**
```json
{
  "type": "kafka",
  "config": {
    "bootstrapServers": "localhost:9092",
    "topic": "processed-events",
    "keyColumn": "customer_id",
    "valueColumn": "event_payload",
    "options": {
      "compression.type": "gzip",
      "acks": "all"
    }
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
│   ├── sdk-transformations/           # Built-in transformations
│   │   ├── foundational/
│   │   │   ├── SelectTransform.java
│   │   │   ├── FilterTransform.java
│   │   │   └── ...
│   │   └── connector/
│   │       ├── FileSource.java
│   │       └── FileSink.java
│   │
│   ├── sdk-connectors/                # Data connectors
│   │   ├── S3Connector.java
│   │   ├── KafkaConnector.java
│   │   └── ...
│   │
│   └── sdk-rules/                     # Rule engine
│       ├── RuleExecutor.java
│       └── BankingRuleTemplates.java
│
├── sdk-listeners/                     # Generic Spark Listeners 🆕
│   ├── LoggingSparkListener.java
│   ├── MetricsSparkListener.java
│   └── ...
│
├── sdk-orchestrator/                  # Workflow orchestration 🆕
│   ├── Pipeline.java                  # Fluent API
│   ├── core/
│   │   ├── Job.java
│   │   ├── JobContext.java
│   │   └── JobResult.java
│   ├── dag/
│   │   └── DAG.java                   # DAG engine
│   ├── executor/
│   │   └── DAGExecutor.java           # Parallel execution
│   ├── retry/
│   │   └── RetryPolicy.java           # Retry strategies
│   ├── sla/
│   │   └── SLAMonitor.java            # SLA monitoring
│   └── scheduler/
│       └── CronScheduler.java         # Cron scheduling
│
└── pipeline-examples/                 # Working examples
    ├── BasicPipelineExample.java
    ├── ComprehensiveBankingPipeline.java
    └── SimpleETLPipeline.java
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

### 5. Advanced Banking Pipeline Example
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

### 6. Phase 3 Comprehensive Example 🆕
Comprehensive Phase 3 example demonstrating all new features.

```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.Phase3ComprehensiveExample"
```

**Features demonstrated:**
- **Cloud & Streaming Connectors**:
  - AWS S3 source and sink
  - Apache Kafka streaming integration
- **Advanced Banking Transformations**:
  - Multi-currency conversion with exchange rates
  - Comprehensive KYC validation
  - Multi-factor risk assessment
  - Intelligent transaction categorization
- **Advanced Data Transformations**:
  - Pivot (long to wide format)
  - Unpivot (wide to long format)
  - Flatten nested JSON/struct data
- **End-to-End Workflows**:
  - International transaction processing
  - Customer compliance validation
  - Risk profiling and scoring
  - Category-based spending analysis

## Modular Architecture 🏗️

The platform is designed with **SDK independence** and **modularity** in mind. You can use components independently or together.

### Core SDK (Standalone)
Use these modules without any platform dependencies:

```xml
<!-- Core interfaces -->
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-api</artifactId>
</dependency>

<!-- Execution engine -->
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-core</artifactId>
</dependency>

<!-- 50+ built-in transformations -->
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-transformations</artifactId>
</dependency>

<!-- Data connectors (File, JDBC, S3, Kafka) -->
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-connectors</artifactId>
</dependency>
```

### Platform Services (Coming Soon)
Enterprise features as microservices:
- `orchestrator-service`: Workflow orchestration & scheduling
- `catalog-service`: Metadata management & lineage
- `quality-service`: Data quality validation
- `governance-service`: Security, RBAC, audit
- `monitoring-service`: Real-time monitoring & alerts
- `ml-service`: ML model integration

See [ARCHITECTURE.md](ARCHITECTURE.md) and [REFACTORING_PLAN.md](REFACTORING_PLAN.md) for complete architecture details.

## SDK Orchestrator - Workflow Management 🆕

**DAG-based workflow orchestration** for managing multi-job data pipelines - similar to Airflow, Prefect, and Dagster, but lightweight and Java-native.

### What is SDK Orchestrator?

A **lightweight workflow orchestration engine** that manages complex data pipelines with multiple jobs and dependencies.

**Key Difference from Spark DAG:**
- **Spark's Internal DAG**: Optimizes transformations *within* a single job (`filter → join → groupBy`)
- **SDK Orchestrator DAG**: Manages *multiple* independent jobs (`Ingest Job → Transform Job → Load Job`)
- **Think**: Spark DAG = car engine (internal), Orchestrator DAG = traffic system (external)

### Quick Start

```java
import com.enterprise.pipeline.orchestrator.*;

// Create and run a simple ETL pipeline
DAGExecutionResult result = Pipeline.create("daily-etl")
    .addJob("ingest", ingestJob)
    .addJob("transform", transformJob)
        .dependsOn("ingest")
    .addJob("load", loadJob)
        .dependsOn("transform")
    .retry(3)
    .sla(Duration.ofMinutes(30))
    .run();

System.out.println("Status: " + result.getOverallStatus());
```

### Core Features

**1. DAG-Based Workflows** - Define job dependencies with automatic parallel execution:
```
     A
    / \
   B   C
    \ /
     D

Level 0: A runs
Level 1: B and C run in parallel (after A completes)
Level 2: D runs (after B and C complete)
```

**2. Retry Policies** - Automatic retry with multiple strategies:
- Exponential backoff: `RetryPolicy.exponentialBackoff(5)`
- Fixed delay: `RetryPolicy.fixedDelay(3, Duration.ofSeconds(10))`
- Linear backoff: `RetryPolicy.linearBackoff(4, Duration.ofSeconds(5))`

**3. SLA Monitoring** - Track execution time and alert on violations:
```java
SLAMonitor monitor = SLAMonitor.builder()
    .threshold(Duration.ofHours(1))
    .onViolation(violation -> sendSlackAlert(violation))
    .build();
```

**4. Cron Scheduling** - Schedule workflows to run automatically:
```java
Pipeline.create("daily-reports")
    .addJob("generate", reportJob)
    .schedule("0 0 * * *")  // Daily at midnight
    .startScheduled();
```

**5. Parallel Execution** - Jobs at the same dependency level run in parallel:
```java
DAGExecutor executor = new DAGExecutor(4);  // 4 parallel threads
```

### Why SDK Orchestrator vs Airflow?

| Feature | SDK Orchestrator | Airflow |
|---------|------------------|---------|
| **Language** | Java | Python |
| **Deployment** | Embedded (single JAR) | Separate cluster |
| **Infrastructure** | None (in-memory) | PostgreSQL, web server, scheduler |
| **Type Safety** | Compile-time | Runtime |
| **Best For** | Embedded workflows, Java apps | Enterprise workflows, large teams |

### When to Use SDK Orchestrator

✅ Building a **platform/SDK** with embedded orchestration
✅ **Java-only** environment (no Python allowed)
✅ **Simple workflows** (Airflow is overkill)
✅ **Lightweight deployments** (no infrastructure overhead)
✅ **Type-safe** compile-time validation needed

### Complete Example

```java
// Define jobs
Job ingest = new IngestJob();
Job clean = new CleanJob();
Job aggregate = new AggregateJob();
Job load = new LoadJob();

// Build pipeline
DAGExecutionResult result = Pipeline.create("etl")
    .addJob("ingest", ingest)
    .addJob("clean", clean).dependsOn("ingest")
    .addJob("aggregate", aggregate).dependsOn("clean")
    .addJob("load", load).dependsOn("aggregate")
    .withParameters(Map.of("date", LocalDate.now()))
    .retry(RetryPolicy.exponentialBackoff(3))
    .sla(Duration.ofHours(2))
    .maxParallelism(8)
    .run();

// Check results
if (result.isSuccess()) {
    System.out.println("Pipeline succeeded!");
} else {
    result.getJobResults().forEach((jobId, jobResult) -> {
        if (jobResult.isFailure()) {
            System.err.println("Failed job: " + jobId);
        }
    });
}
```

### Running the Example

```bash
cd sdk-orchestrator
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.orchestrator.examples.SimpleETLPipeline"
```

### Documentation

📖 **[sdk-orchestrator/README.md](sdk-orchestrator/README.md)** - Complete guide covering:
- Architecture and components
- Job implementation guide
- DAG engine internals
- Retry policies and SLA monitoring
- Cron scheduling examples
- Airflow comparison
- When to use each solution

---

## UI-Based Rule Engine 🎯

Define data validation, transformation, and business rules through a visual interface without writing code.

### Rule Types

1. **Validation Rules** - Data quality checks
   ```json
   {
     "ruleType": "VALIDATION",
     "ruleName": "validate_loan_amount",
     "conditions": [{
       "field": "loan_amount",
       "operator": "BETWEEN",
       "values": [1000, 1000000]
     }],
     "severity": "ERROR"
   }
   ```

2. **Transformation Rules** - Calculate new values
   ```json
   {
     "ruleType": "TRANSFORMATION",
     "ruleName": "calculate_emi",
     "expression": {
       "formula": "P * r * (1+r)^n / ((1+r)^n-1)",
       "variables": {
         "P": "loan_amount",
         "r": "monthly_rate",
         "n": "tenure_months"
       }
     },
     "outputColumn": "monthly_emi"
   }
   ```

3. **Business Rules** - Conditional logic
   ```json
   {
     "ruleType": "BUSINESS",
     "ruleName": "loan_approval",
     "conditions": {
       "type": "AND",
       "rules": [
         {"field": "credit_score", "operator": ">=", "value": 650},
         {"field": "dti", "operator": "<", "value": 0.43}
       ]
     },
     "actions": [
       {"field": "status", "value": "APPROVED"}
     ]
   }
   ```

### Banking Rule Templates

Pre-built templates for common banking scenarios:
- **Loan Approval**: Credit score + DTI validation
- **Fraud Detection**: Multi-factor risk scoring
- **KYC Compliance**: Document verification
- **Credit Scoring**: Behavior-based scoring
- **Transaction Categorization**: Auto-categorize spending

### Usage

```java
// Load pre-built template
BusinessRule rule = BankingRuleTemplates.loanApprovalRule();

// Execute on dataset
RuleExecutor executor = new RuleExecutor();
Dataset<Row> results = executor.execute(applications, rule);
```

See [RULE_ENGINE.md](RULE_ENGINE.md) for complete documentation and [RULE_ENGINE_IMPLEMENTATION.md](RULE_ENGINE_IMPLEMENTATION.md) for implementation guide.

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

### Phase 3 (✅ Completed)
- ✅ S3 connector (with AWS SDK integration)
- ✅ Kafka connector (source & sink)
- ✅ Additional banking transformations (9+ total):
  - ✅ Currency conversion with exchange rates
  - ✅ Comprehensive risk assessment
  - ✅ KYC validation with compliance levels
  - ✅ Transaction categorization (14+ categories)
- ✅ Advanced transformations:
  - ✅ Pivot (long to wide format)
  - ✅ Unpivot (wide to long format)
  - ✅ Flatten nested structures
- ✅ Phase 3 comprehensive example
- ✅ 50+ total transformations

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
