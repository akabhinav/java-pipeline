# Generic Spark Listeners - ALL Industries

**Production-ready Spark Listeners for monitoring, auditing, and performance tracking across ALL industries.**

---

## 🎯 What Are Spark Listeners?

Spark Listeners are **event-driven hooks** that monitor Spark applications in real-time without modifying your core pipeline code.

**Benefits:**
- ✅ Non-invasive monitoring (no code changes needed)
- ✅ Real-time metrics collection
- ✅ Production debugging
- ✅ Compliance and auditing
- ✅ Performance optimization
- ✅ Automated alerting

---

## 📦 5 Generic Listeners Included

### 1. **PerformanceMetricsListener**

**What it does**: Tracks job/stage/task execution performance

**Metrics Collected:**
- Job execution time
- Stage execution time
- Task metrics (CPU time, memory, GC)
- Shuffle read/write volumes
- Input/output records
- Slow task detection

**Use Cases:**
- SLA monitoring
- Performance regression detection
- Capacity planning
- Resource optimization

**Example:**
```java
PerformanceMetricsListener perfListener = new PerformanceMetricsListener();
spark.sparkContext().addSparkListener(perfListener);

// Run your pipeline...

// Get metrics
PerformanceMetricsListener.JobMetrics metrics = perfListener.getJobMetrics(jobId);
System.out.println("Job duration: " + metrics.getDuration() + " ms");
```

---

### 2. **DataQualityListener**

**What it does**: Monitors data quality through pipeline stages

**Metrics Tracked:**
- Row counts (input vs output)
- Null percentages per column
- Duplicate records
- Data loss detection
- Quality score (0-100)

**Use Cases:**
- Detect data quality issues early
- Track data loss
- Monitor null value trends
- Quality score tracking

**Example:**
```java
DataQualityListener qualityListener = new DataQualityListener(spark);
spark.sparkContext().addSparkListener(qualityListener);

// Track quality at each stage
Dataset<Row> input = spark.read().csv("input.csv");
qualityListener.trackQuality("input_stage", input);

Dataset<Row> filtered = input.filter("age >= 18");
qualityListener.trackQuality("filtered_stage", filtered);

// Compare stages
qualityListener.compareStages("input_stage", "filtered_stage");

// Get metrics
QualityMetrics metrics = qualityListener.getMetrics("filtered_stage");
System.out.println("Quality Score: " + metrics.getQualityScore());
```

---

### 3. **JobFailureListener**

**What it does**: Captures job failures with retry logic and alerting

**Features:**
- Full stack trace capture
- Task failure tracking
- Configurable retry logic
- Failure pattern detection
- Automatic alerts

**Use Cases:**
- Prevent data loss
- Automatic recovery
- Failure notifications
- Production debugging

**Example:**
```java
// With auto-retry enabled (max 3 retries)
JobFailureListener failureListener = new JobFailureListener(3, true);
spark.sparkContext().addSparkListener(failureListener);

// Run your pipeline...

// Check failures
JobFailureListener.FailureSummary summary = failureListener.getFailureSummary();
System.out.println("Total job failures: " + summary.getTotalJobFailures());
System.out.println("Total task failures: " + summary.getTotalTaskFailures());
```

---

### 4. **AuditTrailListener**

**What it does**: Complete audit logging for regulatory compliance

**Audit Records:**
- Who (user/application)
- What (job/query executed)
- When (timestamp)
- Where (cluster/environment)
- Status (success/failure)
- Data classification (PII/PCI/PHI)

**Compliance Standards:**
- SOX, GDPR, HIPAA, PCI-DSS
- Basel III (Banking)
- 21 CFR Part 11 (Pharma)

**Use Cases:**
- Regulatory audits
- Security investigations
- Compliance reporting
- Data access tracking

**Example:**
```java
AuditTrailListener auditListener = new AuditTrailListener("production", true);
spark.sparkContext().addSparkListener(auditListener);

// Manual audit events
auditListener.recordDataAccess(
    "customers_table",
    "READ",
    "PII",  // Data classification
    "Query customer data for monthly report"
);

// Get audit trail
List<AuditRecord> trail = auditListener.getAuditTrail();
```

---

### 5. **DataLineageListener**

**What it does**: Tracks data transformations for impact analysis

**Lineage Tracking:**
- Source datasets
- Transformations applied
- Destination datasets
- Column-level lineage
- Visual lineage graph

**Use Cases:**
- Impact analysis ("What breaks if this changes?")
- Root cause analysis
- Data governance
- Auto-generate documentation

**Example:**
```java
DataLineageListener lineageListener = new DataLineageListener();
spark.sparkContext().addSparkListener(lineageListener);

// Track lineage
Dataset<Row> input = spark.read().csv("input.csv");
lineageListener.trackSource("input_data", "file://input.csv", input);

Dataset<Row> filtered = input.filter("age > 18");
lineageListener.trackTransformation("filtered_data", "filter", List.of("input_data"), filtered);

filtered.write().parquet("output.parquet");
lineageListener.trackSink("output_data", "file://output.parquet", "filtered_data");

// Get upstream dependencies
List<LineageNode> upstream = lineageListener.getUpstreamDependencies("output_data");

// Export to Graphviz DOT format
String dotGraph = lineageListener.exportToDOT();
```

---

## 🚀 Quick Start

### 1. Add Dependency

```xml
<dependency>
    <groupId>com.enterprise.pipeline</groupId>
    <artifactId>sdk-listeners</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### 2. Register Listeners

```java
import com.enterprise.pipeline.listeners.*;

// Create Spark session
SparkSession spark = SparkSession.builder()
    .appName("My Pipeline")
    .master("local[*]")
    .getOrCreate();

// Register ALL listeners
PerformanceMetricsListener perfListener = new PerformanceMetricsListener();
DataQualityListener qualityListener = new DataQualityListener(spark);
JobFailureListener failureListener = new JobFailureListener(3, true);
AuditTrailListener auditListener = new AuditTrailListener("production", true);
DataLineageListener lineageListener = new DataLineageListener();

spark.sparkContext().addSparkListener(perfListener);
spark.sparkContext().addSparkListener(qualityListener);
spark.sparkContext().addSparkListener(failureListener);
spark.sparkContext().addSparkListener(auditListener);
spark.sparkContext().addSparkListener(lineageListener);

// Run your pipeline...
```

### 3. Use in Pipeline

```java
// Track data quality
Dataset<Row> data = spark.read().csv("input.csv");
qualityListener.trackQuality("raw_data", data);

// Track lineage
lineageListener.trackSource("raw_data", "file://input.csv", data);

// Your transformations...
Dataset<Row> cleaned = data.filter("value IS NOT NULL");
qualityListener.trackQuality("cleaned_data", cleaned);
lineageListener.trackTransformation("cleaned_data", "filter", List.of("raw_data"), cleaned);

// Track audit
auditListener.recordDataAccess("input.csv", "READ", null, "Processing daily batch");

// Get performance metrics
PerformanceMetricsListener.JobMetrics metrics = perfListener.getJobMetrics(jobId);
System.out.println("Job took: " + metrics.getDuration() + " ms");
```

---

## 📊 Complete Example

See full working example: [ListenersExample.java](../pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ListenersExample.java)

```java
public class ListenersExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Listeners Example")
            .master("local[*]")
            .getOrCreate();

        // Register all listeners
        PerformanceMetricsListener perfListener = new PerformanceMetricsListener();
        DataQualityListener qualityListener = new DataQualityListener(spark);
        JobFailureListener failureListener = new JobFailureListener();
        AuditTrailListener auditListener = new AuditTrailListener();
        DataLineageListener lineageListener = new DataLineageListener();

        spark.sparkContext().addSparkListener(perfListener);
        spark.sparkContext().addSparkListener(qualityListener);
        spark.sparkContext().addSparkListener(failureListener);
        spark.sparkContext().addSparkListener(auditListener);
        spark.sparkContext().addSparkListener(lineageListener);

        // Run pipeline with tracking
        Dataset<Row> input = spark.read().csv("data/input.csv");

        // Track everything
        qualityListener.trackQuality("input", input);
        lineageListener.trackSource("input", "file://data/input.csv", input);
        auditListener.recordDataAccess("input.csv", "READ", null, "Load input data");

        // Transform
        Dataset<Row> filtered = input.filter("age > 18");
        qualityListener.trackQuality("filtered", filtered);
        lineageListener.trackTransformation("filtered", "filter", List.of("input"), filtered);

        // Write
        filtered.write().mode("overwrite").parquet("data/output.parquet");
        lineageListener.trackSink("output", "file://data/output.parquet", "filtered");
        auditListener.recordDataAccess("output.parquet", "WRITE", null, "Write results");

        // Print results
        System.out.println("=== Performance Metrics ===");
        perfListener.getAllJobMetrics().forEach((id, metrics) ->
            System.out.println("Job " + id + ": " + metrics.getDuration() + " ms"));

        System.out.println("\n=== Data Quality ===");
        qualityListener.getAllMetrics().forEach((stage, metrics) ->
            System.out.println(stage + ": Score = " + metrics.getQualityScore()));

        System.out.println("\n=== Failures ===");
        System.out.println(failureListener.getFailureSummary());

        System.out.println("\n=== Audit Trail ===");
        auditListener.getAuditTrail().forEach(System.out::println);

        System.out.println("\n=== Data Lineage (DOT) ===");
        System.out.println(lineageListener.exportToDOT());

        spark.stop();
    }
}
```

---

## 🏭 Industry Use Cases

### Healthcare (HIPAA Compliance)
```java
AuditTrailListener auditListener = new AuditTrailListener("prod", true);
auditListener.recordDataAccess("patient_records", "READ", "PHI", "Monthly reporting");
```

### Finance (SOX, Basel III)
```java
AuditTrailListener auditListener = new AuditTrailListener("prod", true);
auditListener.recordDataAccess("transactions", "READ", "PCI", "Fraud detection");
```

### Retail (GDPR)
```java
AuditTrailListener auditListener = new AuditTrailListener("prod", true);
auditListener.recordDataAccess("customer_data", "READ", "PII", "Marketing analysis");
```

### Manufacturing (Quality Control)
```java
DataQualityListener qualityListener = new DataQualityListener(spark);
QualityMetrics metrics = qualityListener.trackQuality("sensor_data", sensorData);
if (metrics.getQualityScore() < 90) {
    alert("Low data quality detected!");
}
```

---

## 🔧 Configuration

### Programmatic Configuration

```java
// Performance listener with custom thresholds
PerformanceMetricsListener perfListener = new PerformanceMetricsListener() {
    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
        // Custom alert logic
        if (getJobMetrics(jobEnd.jobId()).getDuration() > 600000) {
            sendAlert("Job exceeded 10 minute SLA");
        }
    }
};

// Failure listener with retry
JobFailureListener failureListener = new JobFailureListener(5, true);  // 5 retries

// Audit listener with file logging
AuditTrailListener auditListener = new AuditTrailListener("production", true);
```

### Spark Configuration (spark-defaults.conf)

```properties
# Register listeners in Spark config
spark.extraListeners=com.enterprise.pipeline.listeners.PerformanceMetricsListener,\
                     com.enterprise.pipeline.listeners.JobFailureListener
```

---

## 📈 Metrics & Reporting

All listeners provide metrics that can be exported to:
- **Prometheus** (add Prometheus exporter)
- **Graphite** (add Graphite reporter)
- **CloudWatch** (add AWS CloudWatch integration)
- **Datadog** (add Datadog integration)
- **Custom dashboards** (Grafana, Kibana)

---

## 🐛 Troubleshooting

### Listener not firing events
```java
// Verify listener is registered
spark.sparkContext().listenerBus().listeners().foreach(System.out::println);
```

### High memory usage
```java
// Limit metrics retention
PerformanceMetricsListener perfListener = new PerformanceMetricsListener() {
    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
        // Clear old metrics
        if (getAllJobMetrics().size() > 100) {
            getAllJobMetrics().clear();
        }
    }
};
```

---

## 🎓 Best Practices

1. **Always use Performance Listener** - Essential for production
2. **Enable Audit Listener for compliance** - Required for regulated industries
3. **Track Data Quality at boundaries** - Input, output, and critical transformations
4. **Use Data Lineage for debugging** - Helps with root cause analysis
5. **Configure Failure Listener retry carefully** - Too many retries can waste resources

---

## 📚 Additional Resources

- [Spark Listener API Documentation](https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListener.html)
- [Example Pipeline](../pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ListenersExample.java)
- [Main Project README](../README.md)

---

**These listeners work for ANY industry - healthcare, finance, retail, manufacturing, logistics, media, telecom, and more!**
