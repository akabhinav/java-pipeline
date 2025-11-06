# Pipeline Examples

This module contains comprehensive examples demonstrating all features of the Enterprise Data Pipeline Platform.

## Available Examples

### 1. ComprehensiveBankingPipeline ⭐ **[START HERE]**

**Complete end-to-end demonstration of ALL platform features.**

This pipeline showcases:
- ✅ All 4 rule types (Validation, Transformation, Business, Filter)
- ✅ Data loading from multiple sources (CSV, S3, Kafka simulation)
- ✅ 50+ transformations (basic + banking + advanced)
- ✅ Complete banking workflows (loan approval, fraud detection, KYC)
- ✅ Real-world banking scenarios with sample data

**5 Complete Scenarios:**

1. **Loan Application Processing** - End-to-end loan approval workflow
   - Data validation
   - Customer enrichment (S3)
   - Credit bureau enrichment
   - Credit scoring
   - Business rule-based approval
   - EMI calculation
   - Fraud detection
   - Multi-sink output

2. **Real-Time Fraud Detection** - Transaction monitoring
   - Kafka stream simulation
   - Fraud scoring rules
   - High-value transaction filtering

3. **Customer 360 View** - Data enrichment from multiple sources
   - Multi-source data integration
   - Transaction aggregation
   - Customer lifetime value calculation

4. **Regulatory Compliance** - KYC validation
   - Document verification
   - Compliance status checks
   - Rejection reason tracking

5. **Advanced Analytics** - Interest rate determination
   - Risk-based pricing
   - Revenue projection
   - Portfolio analysis

**How to Run:**

```bash
# From project root
cd pipeline-examples

# Run with Maven
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.ComprehensiveBankingPipeline"

# Or with Spark Submit
spark-submit \
  --class com.enterprise.pipeline.examples.ComprehensiveBankingPipeline \
  --master local[*] \
  target/pipeline-examples-1.0-SNAPSHOT.jar
```

**Expected Output:**
- ✅ 80+ lines loaded across all scenarios
- ✅ 15+ validation rules executed
- ✅ 10+ business rules applied
- ✅ Detailed statistics and results for each scenario
- ✅ Clear pass/fail indicators

---

### 2. BasicPipelineExample

Simple example demonstrating core SDK usage without rules.

**Features:**
- Basic data loading
- Simple transformations
- Data aggregation
- Output writing

**How to Run:**
```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.BasicPipelineExample"
```

---

### 3. AdvancedBankingPipelineExample

Banking-specific transformations and calculations.

**Features:**
- 40+ banking transformations
- Credit scoring algorithms
- Loan calculations
- Risk assessment
- Fraud detection patterns

**How to Run:**
```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.AdvancedBankingPipelineExample"
```

---

### 4. CloudStreamingExample

Cloud and streaming connector examples.

**Features:**
- S3 source/sink configuration
- Kafka producer/consumer setup
- Delta Lake integration
- Real-time streaming patterns

**How to Run:**
```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.CloudStreamingExample"
```

---

### 5. RuleEngineExample

Focused demonstration of the UI-based rule engine.

**Features:**
- All 4 rule types with examples
- Banking rule templates
- Condition logic (AND/OR/NOT)
- Rule chaining
- 15+ operators

**How to Run:**
```bash
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.RuleEngineExample"
```

---

## Quick Start Guide

### Prerequisites

1. **Java 17+**
   ```bash
   java -version
   # Should show: java version "17" or higher
   ```

2. **Apache Spark 3.5+**
   ```bash
   spark-submit --version
   # Should show: version 3.5.0 or higher
   ```

3. **Maven 3.8+**
   ```bash
   mvn -version
   # Should show: Apache Maven 3.8+
   ```

### Build the Project

```bash
# From project root
mvn clean install -DskipTests

# Verify build
ls -lh pipeline-examples/target/*.jar
```

### Run the Comprehensive Pipeline (Recommended)

```bash
cd pipeline-examples

# Option 1: Maven exec
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.ComprehensiveBankingPipeline"

# Option 2: Direct Java execution
java -cp "target/pipeline-examples-1.0-SNAPSHOT.jar:target/lib/*" \
  com.enterprise.pipeline.examples.ComprehensiveBankingPipeline

# Option 3: Spark submit (for cluster deployment)
spark-submit \
  --class com.enterprise.pipeline.examples.ComprehensiveBankingPipeline \
  --master local[4] \
  --driver-memory 4g \
  --executor-memory 4g \
  target/pipeline-examples-1.0-SNAPSHOT.jar
```

---

## Understanding the Output

### Successful Execution Indicators

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

  ... (continues for all steps)

  📊 FINAL RESULTS:
  (Table showing loan approval/rejection with reasons)

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

### What Each Scenario Tests

| Scenario | Features Tested | Key Metrics |
|----------|----------------|-------------|
| Loan Processing | All 4 rule types, enrichment, validation | Approval rate, fraud detection |
| Fraud Detection | Real-time rules, filtering | Fraud flag rate |
| Customer 360 | Multi-source joins, aggregation | Customer LTV |
| Compliance | KYC rules, regulatory checks | Compliance rate |
| Analytics | Interest rate rules, revenue projection | Rate distribution |

---

## Customization Guide

### Adding Your Own Data

Replace the `createXXXData()` methods in `ComprehensiveBankingPipeline.java`:

```java
// Instead of sample data
private static Dataset<Row> createLoanApplicationData(SparkSession spark) {
    // Load from your CSV/Parquet/S3
    return spark.read()
        .option("header", "true")
        .csv("s3://your-bucket/loan-applications.csv");
}
```

### Adding Custom Rules

```java
// Create your own validation rule
ValidationRule myRule = ValidationRule.builder()
    .ruleName("my_custom_validation")
    .conditions(ConditionGroup.and(
        new Condition("amount", Operator.GREATER_THAN, 1000),
        new Condition("status", Operator.EQUALS, "ACTIVE")
    ))
    .severity(Severity.ERROR)
    .outputColumn("my_validation_result")
    .build();

// Execute
RuleExecutor executor = new RuleExecutor();
Dataset<Row> result = executor.execute(data, myRule);
```

### Connecting to Real S3

```java
// Add S3 configuration
SparkSession spark = SparkSession.builder()
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY")
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate();

// Use S3Source
S3Source source = new S3Source.Builder()
    .bucket("your-bucket")
    .key("path/to/data.parquet")
    .format("parquet")
    .build();

Dataset<Row> data = source.read(spark);
```

### Connecting to Real Kafka

```java
// Add Kafka configuration
Properties kafkaProps = new Properties();
kafkaProps.put("bootstrap.servers", "localhost:9092");
kafkaProps.put("group.id", "banking-pipeline");

KafkaSource source = new KafkaSource.Builder()
    .topic("loan-applications")
    .kafkaParams(kafkaProps)
    .build();

Dataset<Row> stream = source.readStream(spark);
```

---

## Troubleshooting

### Common Issues

**1. ClassNotFoundException**
```
Error: Could not find or load main class
```
**Solution:** Rebuild the project
```bash
mvn clean install -DskipTests
```

**2. Out of Memory**
```
java.lang.OutOfMemoryError: Java heap space
```
**Solution:** Increase driver memory
```bash
spark-submit --driver-memory 8g --class ...
```

**3. Spark SQL Analysis Exception**
```
org.apache.spark.sql.AnalysisException: Column 'xxx' not found
```
**Solution:** Check data schema and column names in your rules

**4. Module Not Found**
```
[ERROR] Failed to execute goal: package sdk-rules does not exist
```
**Solution:** Build from root
```bash
cd /path/to/java-pipeline
mvn clean install -DskipTests
```

---

## Performance Tips

### For Large Datasets

1. **Partition your data:**
   ```java
   data.repartition(200).write()...
   ```

2. **Cache intermediate results:**
   ```java
   Dataset<Row> enriched = data.join(customers, "id");
   enriched.cache();
   ```

3. **Use broadcast joins for small tables:**
   ```java
   import static org.apache.spark.sql.functions.broadcast;
   data.join(broadcast(smallTable), "id")
   ```

4. **Enable adaptive query execution:**
   ```java
   spark.conf().set("spark.sql.adaptive.enabled", "true");
   ```

### For Rule-Heavy Pipelines

1. **Disable unused rules:**
   ```java
   rule.enabled(false);
   ```

2. **Order rules by priority:**
   ```java
   rules.sort(Comparator.comparing(Rule::priority));
   ```

3. **Use filter rules early to reduce data volume**

---

## Next Steps

After running the comprehensive pipeline:

1. **Review the code** in `ComprehensiveBankingPipeline.java`
2. **Customize with your data** - Replace sample data with real sources
3. **Add custom rules** - Create business-specific validation and transformation rules
4. **Integrate with UI** - Build React/Angular UI using the JSON rule schemas
5. **Deploy to cluster** - Use spark-submit with YARN/Kubernetes
6. **Monitor performance** - Add metrics and logging

---

## Support

For issues or questions:
- Check the main [README.md](../README.md)
- Review [ARCHITECTURE.md](../ARCHITECTURE.md) for design details
- See [RULE_ENGINE.md](../RULE_ENGINE.md) for rule engine documentation

---

## License

Copyright © 2024 Enterprise Pipeline Platform
