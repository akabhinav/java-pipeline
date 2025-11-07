# SDK Data Quality Framework

Comprehensive data quality assessment framework for Apache Spark datasets. Profile data, validate quality metrics, enforce business rules, and detect anomalies - all with a simple fluent API.

## Features

### 1. 📊 Data Profiling
- **Column Statistics**: Min, max, mean, median, standard deviation
- **Null Analysis**: Count and percentage of null values
- **Uniqueness**: Distinct value counts and cardinality
- **String Metrics**: Min/max/average length for text columns
- **Value Distributions**: Top N most frequent values
- **Quality Scores**: Automatic completeness and uniqueness percentages

### 2. 📈 Quality Metrics
- **Completeness**: Percentage of non-null values
- **Uniqueness**: Percentage of distinct values
- **Accuracy**: Validation against patterns, ranges, or custom rules
- **Consistency**: Cross-field validation rules
- **Weighted Scoring**: Calculate overall quality score with custom weights

### 3. ✅ Quality Rules Engine
- **Not Null Rules**: Validate required fields
- **Range Rules**: Numeric bounds validation
- **Regex Rules**: Pattern matching for emails, phones, SSNs, etc.
- **Uniqueness Rules**: Detect duplicates
- **Custom Rules**: Implement your own validation logic
- **Severity Levels**: INFO, WARNING, ERROR, CRITICAL

### 4. 🔍 Anomaly Detection
- **Statistical Outliers**: Z-score and IQR methods
- **Volume Anomalies**: Unexpected row counts
- **Pattern Detection**: Identify unusual patterns
- **Customizable Thresholds**: Configure sensitivity

### 5. 📄 Comprehensive Reports
- **Unified Reports**: All quality assessments in one view
- **Overall Quality Score**: Single metric for data quality (0-100)
- **Grading System**: A/B/C/D/F grades based on score
- **Detailed Summaries**: Profiling, metrics, rules, and anomalies

---

## Quick Start

### Basic Usage

```java
import com.enterprise.pipeline.quality.DataQualityFramework;
import com.enterprise.pipeline.quality.report.QualityReport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// Create Spark session and load data
Dataset<Row> dataset = spark.read().parquet("s3://data-lake/silver/customers");

// Comprehensive quality assessment
QualityReport report = DataQualityFramework.assess(dataset, "customers")
    .profile()                          // Enable data profiling
    .withMetrics(metrics -> metrics
        .addCompleteness("email")       // Check email completeness
        .addUniqueness("customer_id")   // Check ID uniqueness
    )
    .withRules(rules -> rules
        .addNotNull("customer_id")      // customer_id cannot be null
        .addRange("age", 18, 120)       // age must be 18-120
        .addRegex("email", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    )
    .detectAnomalies()                  // Find statistical outliers
    .generate();

// Print comprehensive report
System.out.println(report);

// Check if quality passes threshold
if (report.passes(80.0)) {
    System.out.println("✓ Data quality is acceptable");
} else {
    System.out.println("✗ Data quality issues detected");
}
```

### One-Line Complete Assessment

```java
QualityReport report = DataQualityFramework.completeAssessment(dataset, "customers");
System.out.println("Quality Score: " + report.getOverallScore() + "%");
```

---

## Core Components

### 1. Data Profiler

Profile datasets to understand data characteristics:

```java
import com.enterprise.pipeline.quality.profiler.DataProfiler;
import com.enterprise.pipeline.quality.profiler.ProfileResult;

DataProfiler profiler = new DataProfiler();
ProfileResult result = profiler.profile(dataset, "customers");

// Access column profiles
result.getColumnProfiles().forEach(profile -> {
    System.out.println("Column: " + profile.getColumnName());
    System.out.println("  Type: " + profile.getDataType());
    System.out.println("  Null Count: " + profile.getNullCount());
    System.out.println("  Completeness: " + profile.getCompletenessPercent() + "%");
    System.out.println("  Distinct Values: " + profile.getDistinctCount());

    if (profile.isNumeric()) {
        System.out.println("  Min: " + profile.getMin());
        System.out.println("  Max: " + profile.getMax());
        System.out.println("  Mean: " + profile.getMean());
        System.out.println("  StdDev: " + profile.getStdDev());
    }
});
```

**Output Example:**
```
Column: age
  Type: integer
  Null Count: 5
  Completeness: 99.50%
  Distinct Values: 82
  Min: 18.0
  Max: 95.0
  Mean: 42.35
  StdDev: 15.67
```

---

### 2. Quality Metrics

Calculate specific quality dimensions:

```java
import com.enterprise.pipeline.quality.metrics.*;

QualityMetrics metrics = QualityMetrics.builder()
    // Completeness metrics
    .add(new CompletenessMetric("email"))
    .add(new CompletenessMetric("phone"))

    // Uniqueness metrics
    .add(new UniquenessMetric("customer_id"))
    .add(new UniquenessMetric("email"))

    // Accuracy metrics (pattern/range validation)
    .add(AccuracyMetric.regex("email", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"))
    .add(AccuracyMetric.range("age", 18, 120))

    // Consistency metrics (cross-field validation)
    .add(ConsistencyMetric.withExpression("end_date >= start_date"))

    // Weighted metrics (2x weight for critical fields)
    .add(new CompletenessMetric("customer_id"), 2.0)

    .build();

QualityMetrics.QualityMetricsResult result = metrics.calculate(dataset);

System.out.println(result.getSummary());
System.out.println("Overall Score: " + result.getOverallScore() + "%");
```

**Output Example:**
```
Quality Metrics Summary:
============================================================
Completeness(email)                      :  95.30%
Completeness(phone)                      :  88.50%
Uniqueness(customer_id)                  : 100.00%
Uniqueness(email)                        :  99.80%
Accuracy(email)                          :  94.20%
Accuracy(age)                            :  98.70%
Consistency                              :  99.50%
============================================================
OVERALL QUALITY SCORE                    :  96.57%
============================================================
```

---

### 3. Quality Rules Engine

Validate data against business rules:

```java
import com.enterprise.pipeline.quality.rules.*;

RuleEngine engine = RuleEngine.builder()
    // Not null rules
    .addNotNull("customer_id")
    .addNotNull("email")

    // Range rules
    .addRange("age", 18, 120)
    .add(new RangeRule("credit_score", 300, 850))

    // Regex rules
    .addRegex("email", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
    .add(RegexRule.phone("phone_number"))
    .add(RegexRule.zipCode("zip_code"))
    .add(RegexRule.ssn("ssn"))

    // Uniqueness rules
    .addUnique("customer_id")
    .add(new UniqueRule("email", "phone"))

    // Custom rules
    .add(new CustomBusinessRule())

    .failFast(true)  // Stop on critical failures
    .build();

RuleEngine.RuleExecutionResult result = engine.execute(dataset);

if (result.allPassed()) {
    System.out.println("✓ All quality rules passed");
} else {
    System.out.println("✗ " + result.getFailedCount() + " rules failed");

    // Get violation details
    result.getViolations().forEach((ruleName, violation) -> {
        System.out.println("Rule: " + ruleName);
        System.out.println("  Violations: " + violation.getViolationCount());
        System.out.println("  Severity: " + violation.getRule().getSeverity());

        // Show violating rows
        violation.getViolatingRows().show(5);
    });
}
```

**Output Example:**
```
QUALITY RULES EXECUTION SUMMARY
================================================================================
Total Rules: 8
Passed: 6
Failed: 2
Total Violations: 127
================================================================================

VIOLATIONS:
--------------------------------------------------------------------------------
Rule: NotNull(email)
  Description: Column 'email' should not contain null values
  Severity: ERROR
  Violations: 42

Rule: Regex(email)
  Description: Column 'email' should match pattern: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
  Severity: ERROR
  Violations: 85

================================================================================
```

---

### 4. Anomaly Detection

Detect unusual patterns and outliers:

```java
import com.enterprise.pipeline.quality.anomaly.*;

// Statistical outlier detection
AnomalyDetector statisticalDetector = new StatisticalAnomalyDetector(
    3.0,  // Z-score threshold
    1.5,  // IQR multiplier
    StatisticalAnomalyDetector.Method.BOTH
);

List<AnomalyDetector.Anomaly> outliers = statisticalDetector.detect(dataset);

// Volume anomaly detection
AnomalyDetector volumeDetector = new VolumeAnomalyDetector(
    100000,  // Expected row count
    10.0     // ±10% tolerance
);

List<AnomalyDetector.Anomaly> volumeAnomalies = volumeDetector.detect(dataset);

// Print anomalies
outliers.forEach(anomaly -> {
    System.out.println(anomaly.toString());
});
```

**Output Example:**
```
[STATISTICAL_OUTLIER] Value is 4.35 standard deviations from mean (column: age, value: 150.0, score: 4.35) - Mean: 42.35, StdDev: 15.67, Z-Score: 4.35
[IQR_OUTLIER] Value outside IQR bounds (column: salary, value: 950000.0, score: 3.82) - Q1: 45000.00, Q3: 120000.00, IQR: 75000.00, Bounds: [32500.00, 232500.00]
[VOLUME_ANOMALY] Row count is 15.30% higher than expected (column: *, value: 115300, score: 15.30) - Expected: 100000, Actual: 115300, Tolerance: 10.00%
```

---

## Integration with Data Lake

Perfect for validating Data Lake layers:

```java
// Bronze → Silver validation
Dataset<Row> bronzeData = spark.read().format("delta")
    .load("s3a://data-lake/bronze/transactions");

QualityReport bronzeReport = DataQualityFramework.assess(bronzeData, "bronze_transactions")
    .profile()
    .withMetrics(metrics -> metrics
        .addCompleteness("transaction_id")
        .addCompleteness("amount")
        .addCompleteness("timestamp")
    )
    .withRules(rules -> rules
        .addNotNull("transaction_id")
        .addUnique("transaction_id")
        .addRange("amount", 0.01, 1000000.00)
    )
    .detectAnomalies()
    .generate();

// Only proceed to Silver if quality passes
if (bronzeReport.passes(95.0)) {
    // Clean and transform to Silver
    Dataset<Row> silverData = cleanData(bronzeData);
    silverData.write().format("delta")
        .mode("append")
        .save("s3a://data-lake/silver/transactions");
} else {
    System.err.println("Bronze data failed quality checks!");
    System.err.println(bronzeReport);
}
```

---

## Integration with SDK Orchestrator

Use as a quality check job in workflows:

```java
import com.enterprise.pipeline.orchestrator.*;
import com.enterprise.pipeline.quality.*;

public class QualityCheckJob implements Job {
    @Override
    public String getId() { return "quality_check"; }

    @Override
    public String getName() { return "Data Quality Validation"; }

    @Override
    public JobResult execute(JobContext context) throws Exception {
        SparkSession spark = SparkSession.active();
        Dataset<Row> dataset = spark.read().parquet(
            context.getParameter("input_path").orElse("")
        );

        QualityReport report = DataQualityFramework.assess(dataset, "validation")
            .profile()
            .withMetrics(metrics -> /* configure */)
            .withRules(rules -> /* configure */)
            .detectAnomalies()
            .generate();

        if (report.passes(80.0)) {
            return JobResult.builder(getId())
                .status(JobStatus.SUCCESS)
                .output("quality_score", report.getOverallScore())
                .build();
        } else {
            return JobResult.builder(getId())
                .status(JobStatus.FAILED)
                .error(new Exception("Quality check failed: " + report.getOverallScore() + "%"))
                .build();
        }
    }
}

// Use in pipeline
Pipeline.create("etl-with-quality")
    .addJob("ingest", ingestJob)
    .addJob("quality_check", new QualityCheckJob())
        .dependsOn("ingest")
    .addJob("transform", transformJob)
        .dependsOn("quality_check")  // Only runs if quality passes
    .run();
```

---

## Advanced Use Cases

### Custom Quality Rules

```java
public class CustomBusinessRule implements QualityRule {
    @Override
    public String getName() {
        return "Business Logic Rule";
    }

    @Override
    public String getDescription() {
        return "Validates business-specific constraints";
    }

    @Override
    public Dataset<Row> validate(Dataset<Row> dataset) {
        // Return rows that violate the rule
        return dataset.filter(
            "total_purchase_amount > credit_limit AND customer_type = 'REGULAR'"
        );
    }

    @Override
    public RuleSeverity getSeverity() {
        return RuleSeverity.WARNING;
    }
}
```

### Monitoring Data Quality Over Time

```java
// Daily quality monitoring
Dataset<Row> todayData = loadData(LocalDate.now());

QualityReport report = DataQualityFramework.completeAssessment(
    todayData, "daily_transactions_" + LocalDate.now()
);

// Store quality score in metrics database
metricsDB.insert(new QualityMetric(
    LocalDate.now(),
    "transactions",
    report.getOverallScore(),
    report.getProfile().getTotalRows(),
    report.getRulesResult().getFailedCount()
));

// Alert if quality degrades
if (report.getOverallScore() < 85.0) {
    alerting.sendAlert("Data quality degradation detected: " +
        report.getOverallScore() + "%");
}
```

---

## Running the Example

```bash
cd sdk-data-quality
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.quality.examples.DataQualityExample"
```

---

## API Reference

### DataQualityFramework

Main entry point for quality assessment:

| Method | Description |
|--------|-------------|
| `assess(dataset, name)` | Start quality assessment |
| `profile()` | Enable data profiling |
| `withMetrics(configurator)` | Add quality metrics |
| `withRules(configurator)` | Add validation rules |
| `detectAnomalies()` | Enable default anomaly detection |
| `withAnomalyDetector(detector)` | Add custom anomaly detector |
| `detectVolumeAnomalies(count, tolerance)` | Add volume anomaly detection |
| `generate()` | Generate comprehensive report |
| `quickCheck(threshold)` | Returns true/false based on threshold |
| `completeAssessment(dataset, name)` | One-line complete assessment |

### Quality Metrics

- `CompletenessMetric`: Percentage of non-null values
- `UniquenessMetric`: Percentage of distinct values
- `AccuracyMetric`: Pattern/range validation score
- `ConsistencyMetric`: Cross-field validation score

### Quality Rules

- `NotNullRule`: Validate non-null constraint
- `RangeRule`: Validate numeric ranges
- `RegexRule`: Validate string patterns
- `UniqueRule`: Validate uniqueness
- Custom: Implement `QualityRule` interface

### Anomaly Detectors

- `StatisticalAnomalyDetector`: Z-score and IQR outliers
- `VolumeAnomalyDetector`: Unexpected row counts
- Custom: Implement `AnomalyDetector` interface

---

## Best Practices

1. **Profile First**: Always profile new datasets to understand baseline quality
2. **Start Simple**: Begin with basic completeness and uniqueness checks
3. **Iterate**: Add rules incrementally based on profiling insights
4. **Set Thresholds**: Define acceptable quality scores for your use case
5. **Monitor Trends**: Track quality scores over time to detect degradation
6. **Fail Fast**: Use critical rules to stop processing bad data early
7. **Document Rules**: Use descriptive names and severities for business rules
8. **Integrate**: Use quality checks in orchestration workflows

---

## Architecture

```
DataQualityFramework (Fluent API)
       │
       ├─── DataProfiler
       │    └─── ColumnProfile → ProfileResult
       │
       ├─── QualityMetrics
       │    ├─── CompletenessMetric
       │    ├─── UniquenessMetric
       │    ├─── AccuracyMetric
       │    └─── ConsistencyMetric
       │
       ├─── RuleEngine
       │    ├─── NotNullRule
       │    ├─── RangeRule
       │    ├─── RegexRule
       │    └─── UniqueRule
       │
       ├─── AnomalyDetector
       │    ├─── StatisticalAnomalyDetector
       │    └─── VolumeAnomalyDetector
       │
       └─── QualityReport
            └─── Overall Score + Details
```

---

## Dependencies

- Apache Spark 3.5.0
- Apache Commons Math3 3.6.1 (for statistics)
- SLF4J (logging)
- Jackson (JSON serialization)

---

## License

Copyright © 2024 Enterprise Data Pipeline Team
