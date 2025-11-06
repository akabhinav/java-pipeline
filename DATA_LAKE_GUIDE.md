# Data Lake on MinIO - Complete Guide

Build a production-grade **Data Lake** on your local MinIO using **Delta Lake** format!

---

## 🎯 What You'll Build

A complete **Medallion Architecture Data Lake** with:

- **Bronze Layer**: Raw, immutable data (append-only)
- **Silver Layer**: Cleaned, validated, deduplicated data
- **Gold Layer**: Business-ready aggregates and metrics

All stored on **MinIO** (S3-compatible) with **Delta Lake** format providing:
- ✅ **ACID Transactions**
- ✅ **Time Travel** (query historical versions)
- ✅ **Schema Evolution** (add columns without breaking)
- ✅ **Upserts/Merges** (update or insert)
- ✅ **Data Versioning** (full audit trail)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        MinIO (S3-Compatible)                     │
│                      s3a://data-lake/                            │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│   BRONZE     │      │   SILVER     │      │    GOLD      │
│   (Raw)      │──────▶  (Cleaned)   │──────▶ (Aggregated) │
├──────────────┤      ├──────────────┤      ├──────────────┤
│ • Append-only│      │ • Validated  │      │ • Metrics    │
│ • Immutable  │      │ • Deduped    │      │ • Summaries  │
│ • Full history│     │ • Quality    │      │ • Reports    │
│              │      │   scored     │      │              │
└──────────────┘      └──────────────┘      └──────────────┘
```

### Data Flow

1. **Raw Data** → Bronze Layer (with metadata: ingestion_timestamp, source_system)
2. **Bronze** → Silver Layer (cleaned, validated, quality scored)
3. **Silver** → Gold Layer (aggregated for business use)

---

## 🚀 Quick Start

### Step 1: Start Environment

```bash
# Windows
start-local-environment.bat

# Mac/Linux
./start-local-environment.sh
```

This creates:
- MinIO bucket: `s3a://data-lake/`
- Bronze, Silver, Gold directories
- All infrastructure (Kafka, PostgreSQL, Spark cluster)

### Step 2: Run Data Lake Pipeline

```bash
# Windows
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.DataLakePipeline"

# Mac/Linux
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.DataLakePipeline"
```

### Step 3: View Your Data Lake

**MinIO Console**: http://localhost:9001
- Username: `minioadmin`
- Password: `minioadmin`
- Bucket: `data-lake`

You'll see:
```
data-lake/
  ├── bronze/
  │   ├── loan_applications/
  │   │   ├── _delta_log/         # Delta Lake transaction log
  │   │   └── part-*.parquet       # Data files
  │   └── customers/
  │       ├── _delta_log/
  │       └── part-*.parquet
  ├── silver/
  │   ├── loan_applications_clean/
  │   └── customers_clean/
  └── gold/
      ├── loan_summary_by_purpose/
      └── customer_metrics/
```

---

## 📊 What the Pipeline Does

### Bronze Layer - Raw Data Ingestion

**Purpose**: Store raw data exactly as received, immutable

```java
Dataset<Row> rawLoanData = loadRawLoanData(spark);
writeToBronze(rawLoanData, "loan_applications");

// Adds metadata automatically:
// - ingestion_timestamp (when data arrived)
// - source_system (where data came from)
```

**Storage Path**: `s3a://data-lake/bronze/loan_applications/`

**Sample Data**:
| application_id | customer_id | loan_amount | loan_purpose | ingestion_timestamp | source_system |
|----------------|-------------|-------------|--------------|---------------------|---------------|
| LA001 | C001 | 500000.0 | HOME_LOAN | 2025-11-06 10:30 | banking_system |
| LA002 | C002 | 150000.0 | PERSONAL_LOAN | 2025-11-06 10:30 | banking_system |

---

### Silver Layer - Cleaned & Validated

**Purpose**: Remove duplicates, validate quality, add quality scores

```java
Dataset<Row> cleanedLoans = cleanLoans(spark, BRONZE_PATH + "/loan_applications");
writeToSilver(cleanedLoans, "loan_applications_clean");
```

**Cleaning Rules**:
- ✅ Remove duplicates (by application_id)
- ✅ Validate: `loan_amount > 0`
- ✅ Validate: `applicant_age >= 18 AND applicant_age <= 100`
- ✅ Add `data_quality_score = 1.0`
- ✅ Add `cleaned_timestamp`

**Storage Path**: `s3a://data-lake/silver/loan_applications_clean/`

---

### Gold Layer - Business Aggregates

**Purpose**: Create business-ready metrics and summaries

```java
Dataset<Row> loanSummary = createLoanSummary(spark);
writeToGold(loanSummary, "loan_summary_by_purpose");
```

**Aggregations**:
- Total applications per loan purpose
- Total loan amount per purpose
- Average loan amount per purpose
- Average applicant age per purpose

**Storage Path**: `s3a://data-lake/gold/loan_summary_by_purpose/`

**Sample Output**:
| loan_purpose | total_applications | total_amount | avg_amount | avg_applicant_age | updated_at |
|--------------|-------------------|--------------|------------|-------------------|------------|
| HOME_LOAN | 1 | 500000.0 | 500000.0 | 35.0 | 2025-11-06 10:31 |
| PERSONAL_LOAN | 2 | 450000.0 | 225000.0 | 41.5 | 2025-11-06 10:31 |
| BUSINESS_LOAN | 1 | 2000000.0 | 2000000.0 | 45.0 | 2025-11-06 10:31 |

---

## ⚡ Delta Lake Features

### 1. ACID Transactions

Every write is atomic - either fully succeeds or fully fails. No partial writes!

```java
// This either writes ALL records or NONE
data.write()
    .format("delta")
    .mode(SaveMode.Append)
    .save("s3a://data-lake/bronze/loans");
```

### 2. Time Travel

Query any previous version of your data!

```java
// Query version 0 (original data)
Dataset<Row> version0 = spark.read()
    .format("delta")
    .option("versionAsOf", "0")
    .load("s3a://data-lake/silver/loan_applications_clean");

// Query as of specific timestamp
Dataset<Row> yesterday = spark.read()
    .format("delta")
    .option("timestampAsOf", "2025-11-05 10:00:00")
    .load("s3a://data-lake/silver/loan_applications_clean");
```

**View History**:
```java
DeltaTable deltaTable = DeltaTable.forPath(spark, silverPath);
deltaTable.history(5).show();
```

Output:
| version | timestamp | operation | operationMetrics |
|---------|-----------|-----------|------------------|
| 2 | 2025-11-06 10:35 | MERGE | numTargetRowsUpdated=5 |
| 1 | 2025-11-06 10:32 | WRITE | numFiles=2 |
| 0 | 2025-11-06 10:30 | WRITE | numFiles=1 |

### 3. Upserts (Merge)

Update existing records or insert new ones in a single operation!

```java
DeltaTable silverTable = DeltaTable.forPath(spark, silverPath);

silverTable.as("target")
    .merge(
        updates.as("source"),
        "target.application_id = source.application_id"
    )
    .whenMatched()
    .updateAll()      // Update if exists
    .whenNotMatched()
    .insertAll()      // Insert if new
    .execute();
```

**Use Case**: Daily customer updates
- New customers → inserted
- Existing customers → updated with latest info

### 4. Schema Evolution

Add columns without rewriting existing data!

```java
// Add new column to existing table
Dataset<Row> withNewColumn = spark.read()
    .format("delta")
    .load(silverPath)
    .withColumn("risk_category", lit("MEDIUM"));

withNewColumn.write()
    .format("delta")
    .mode(SaveMode.Overwrite)
    .option("mergeSchema", "true")  // Enable schema evolution
    .save(silverPath);
```

**Before**:
| application_id | loan_amount | applicant_age |
|----------------|-------------|---------------|
| LA001 | 500000 | 35 |

**After** (existing records have NULL for new column):
| application_id | loan_amount | applicant_age | risk_category |
|----------------|-------------|---------------|---------------|
| LA001 | 500000 | 35 | NULL |
| LA006 | 300000 | 40 | MEDIUM |

---

## 🔍 Querying Your Data Lake

### From Spark SQL

```java
// Register Delta table
spark.sql("CREATE TABLE loans " +
          "USING DELTA " +
          "LOCATION 's3a://data-lake/silver/loan_applications_clean'");

// Query it
Dataset<Row> highValueLoans = spark.sql(
    "SELECT * FROM loans WHERE loan_amount > 1000000"
);
highValueLoans.show();
```

### From Code

```java
// Read current version
Dataset<Row> current = spark.read()
    .format("delta")
    .load("s3a://data-lake/gold/loan_summary_by_purpose");

// Filter and analyze
current.filter("total_amount > 1000000")
       .orderBy(desc("avg_amount"))
       .show();
```

---

## 📈 Performance & Best Practices

### 1. Partitioning

Partition large tables by frequently filtered columns:

```java
data.write()
    .format("delta")
    .partitionBy("loan_purpose", "year", "month")  // Partition strategy
    .save("s3a://data-lake/bronze/loans");
```

**Benefit**: Queries like `WHERE loan_purpose = 'HOME_LOAN'` only read relevant partitions!

### 2. Compaction (OPTIMIZE)

Combine small files for better read performance:

```java
DeltaTable deltaTable = DeltaTable.forPath(spark, tablePath);
deltaTable.optimize().executeCompaction();
```

**When**: After many small writes (e.g., streaming)

### 3. Vacuum Old Versions

Delete old data files no longer needed (keeps transaction log):

```java
DeltaTable deltaTable = DeltaTable.forPath(spark, tablePath);
deltaTable.vacuum(7);  // Keep 7 days of history
```

**Warning**: You can't time travel to vacuumed versions!

### 4. Z-Order Clustering

Co-locate related data for faster queries:

```java
deltaTable.optimize()
    .zorder("customer_id", "loan_purpose")
    .executeCompaction();
```

**Benefit**: Queries filtering on these columns are much faster!

---

## 🛠️ Common Operations

### Add New Data (Append)

```java
Dataset<Row> newLoans = loadNewLoanData(spark);

newLoans.write()
    .format("delta")
    .mode(SaveMode.Append)
    .save("s3a://data-lake/bronze/loan_applications");
```

### Replace All Data (Overwrite)

```java
Dataset<Row> correctedData = fixDataIssues(spark);

correctedData.write()
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save("s3a://data-lake/silver/loan_applications_clean");
```

### Update Specific Records

```java
DeltaTable table = DeltaTable.forPath(spark, tablePath);

table.update(
    expr("loan_status = 'PENDING'"),           // Condition
    Map.of("loan_status", lit("APPROVED"))     // Update
);
```

### Delete Records

```java
DeltaTable table = DeltaTable.forPath(spark, tablePath);

table.delete(expr("loan_amount < 0"));  // Delete invalid records
```

---

## 📊 Monitoring Your Data Lake

### Table Statistics

```java
DeltaTable deltaTable = DeltaTable.forPath(spark, tablePath);

// Show history
deltaTable.history(10).show(false);

// Show details
deltaTable.detail().show(false);
```

**Output**:
| format | id | name | location | numFiles | sizeInBytes |
|--------|----|----- |----------|----------|-------------|
| delta | xxx-xxx | null | s3a://... | 3 | 12345 |

### Metrics per Layer

```java
// Bronze Layer
long bronzeRecords = spark.read()
    .format("delta")
    .load("s3a://data-lake/bronze/loan_applications")
    .count();

// Silver Layer
long silverRecords = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean")
    .count();

// Data quality: How many records lost during cleaning?
double qualityRate = (double) silverRecords / bronzeRecords * 100;
System.out.printf("Data Quality: %.2f%% records passed validation\n", qualityRate);
```

---

## 🎓 Example Scenarios

### Scenario 1: Daily Loan Processing

**Goal**: Ingest new loan applications daily, clean, and generate reports

```java
// Day 1: Initial load
Dataset<Row> day1Loans = loadLoans("2025-11-01");
writeToBronze(day1Loans, "loan_applications");

// Day 2: Append new loans
Dataset<Row> day2Loans = loadLoans("2025-11-02");
day2Loans.write()
    .format("delta")
    .mode(SaveMode.Append)
    .save("s3a://data-lake/bronze/loan_applications");

// Clean all data (Bronze → Silver)
Dataset<Row> allLoans = spark.read()
    .format("delta")
    .load("s3a://data-lake/bronze/loan_applications");

Dataset<Row> cleaned = cleanLoans(spark, allLoans);
cleaned.write()
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save("s3a://data-lake/silver/loan_applications_clean");

// Generate daily report (Silver → Gold)
Dataset<Row> report = createDailyReport(spark);
report.write()
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save("s3a://data-lake/gold/daily_loan_report");
```

### Scenario 2: Customer Data Merge

**Goal**: Update customer records daily (upsert)

```java
// Existing customers in Silver layer
DeltaTable silverCustomers = DeltaTable.forPath(spark,
    "s3a://data-lake/silver/customers_clean");

// New/updated customer data
Dataset<Row> dailyUpdates = loadCustomerUpdates(spark);

// Merge: Update existing, insert new
silverCustomers.as("target")
    .merge(
        dailyUpdates.as("source"),
        "target.customer_id = source.customer_id"
    )
    .whenMatched()
    .updateExpr(Map.of(
        "email", "source.email",
        "monthly_income", "source.monthly_income",
        "updated_at", "current_timestamp()"
    ))
    .whenNotMatched()
    .insertAll()
    .execute();
```

### Scenario 3: Audit & Compliance

**Goal**: Prove what data looked like last month

```java
// Regulator asks: "What were customer incomes on Oct 1?"
Dataset<Row> oct1Data = spark.read()
    .format("delta")
    .option("timestampAsOf", "2024-10-01 00:00:00")
    .load("s3a://data-lake/silver/customers_clean");

// Generate report for regulator
oct1Data.write()
    .format("csv")
    .option("header", "true")
    .save("s3a://reports/regulatory/oct1-customer-snapshot.csv");
```

---

## 🔐 Data Governance

### Data Lineage

Track where data came from:

```java
// Bronze: Add source metadata
rawData.withColumn("source_system", lit("banking_system"))
       .withColumn("source_file", input_file_name())
       .withColumn("ingestion_timestamp", current_timestamp())
```

### Data Quality Scoring

```java
// Add quality flags
Dataset<Row> withQuality = silver
    .withColumn("quality_score",
        when(col("email").isNotNull()
            .and(col("monthly_income").gt(0))
            .and(col("age").between(18, 100)), 1.0)
        .otherwise(0.5)
    );
```

### Audit Trail

Every Delta table automatically maintains:
- Who made changes (user info in transaction log)
- When changes occurred (timestamp)
- What changed (operation type: INSERT, UPDATE, DELETE, MERGE)
- How many records affected (operationMetrics)

```java
// View full audit trail
deltaTable.history().show(false);
```

---

## 🐛 Troubleshooting

### Issue 1: "Table already exists"

**Error**: `AnalysisException: Path already exists`

**Solution**: Use `mode(SaveMode.Overwrite)` or `mode(SaveMode.Append)`

```java
data.write()
    .format("delta")
    .mode(SaveMode.Overwrite)  // Replace existing data
    .save(path);
```

### Issue 2: Schema Mismatch

**Error**: `Schema mismatch: expected X but got Y`

**Solution**: Enable schema merging

```java
data.write()
    .format("delta")
    .mode(SaveMode.Append)
    .option("mergeSchema", "true")  // Allow schema evolution
    .save(path);
```

### Issue 3: S3 Connection Failed

**Error**: `Connection refused to MinIO`

**Check**:
1. MinIO is running: `docker ps | grep minio`
2. Credentials are correct: `minioadmin` / `minioadmin`
3. Endpoint is correct: `http://localhost:9000`

**Fix**:
```bash
# Restart MinIO
docker-compose restart minio
```

### Issue 4: Delta Log Corruption

**Error**: `Delta log corrupted at version X`

**Recovery**:
```java
// Restore to last good version
spark.read()
    .format("delta")
    .option("versionAsOf", "X-1")  // Version before corruption
    .load(path)
    .write()
    .format("delta")
    .mode(SaveMode.Overwrite)
    .save(path + "_recovered");
```

---

## 📚 Advanced Topics

### Streaming to Data Lake

```java
// Stream from Kafka → Bronze Layer
spark.readStream()
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "loan-applications")
    .load()
    .writeStream()
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3a://checkpoints/loans")
    .start("s3a://data-lake/bronze/loan_applications");
```

### Multi-Table Transactions

```java
// Update multiple tables atomically (not natively supported)
// Workaround: Use Delta MERGE for dependent updates

DeltaTable loans = DeltaTable.forPath(spark, loansPath);
DeltaTable customers = DeltaTable.forPath(spark, customersPath);

// Update loans
loans.update(...);

// Update related customers
customers.update(...);
```

### Clone Tables (Zero-Copy)

```java
// Deep clone (copy all data)
DeltaTable.clone(
    spark,
    "s3a://data-lake/silver/loans",
    "s3a://data-lake/test/loans",
    false  // isShallow = false
);

// Shallow clone (copy only metadata)
DeltaTable.clone(
    spark,
    "s3a://data-lake/silver/loans",
    "s3a://data-lake/test/loans",
    true  // isShallow = true
);
```

---

## 🚀 Production Deployment

### Differences from Local

| Aspect | Local (MinIO) | Production (S3/ADLS) |
|--------|---------------|----------------------|
| **Storage** | MinIO (9000) | AWS S3, Azure ADLS, GCS |
| **Endpoint** | http://localhost:9000 | https://s3.amazonaws.com |
| **Credentials** | minioadmin/minioadmin | IAM roles, SAS tokens |
| **Bucket** | data-lake | prod-data-lake-company-name |
| **Partitions** | Small (100s) | Large (1000s) |
| **Optimization** | Manual | Scheduled (nightly OPTIMIZE) |

### Production Spark Configuration

```java
SparkSession spark = SparkSession.builder()
    .appName("Production Data Lake")
    .master("spark://prod-cluster:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    // AWS S3
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider")

    // Performance
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.databricks.delta.optimizeWrite.enabled", "true")
    .config("spark.databricks.delta.autoCompact.enabled", "true")

    .getOrCreate();
```

### Scheduled Maintenance

```bash
# Daily: Optimize tables
spark-submit --class MaintenanceJob \
  --conf spark.sql.command="OPTIMIZE delta.`s3a://data-lake/silver/loans`"

# Weekly: Vacuum old versions (keep 30 days)
spark-submit --class MaintenanceJob \
  --conf spark.sql.command="VACUUM delta.`s3a://data-lake/silver/loans` RETAIN 720 HOURS"
```

---

## 📊 Comparison: Delta Lake vs Parquet

| Feature | Parquet (Plain) | Delta Lake |
|---------|----------------|------------|
| **Format** | Column-oriented | Parquet + transaction log |
| **ACID** | ❌ No | ✅ Yes |
| **Time Travel** | ❌ No | ✅ Yes |
| **Schema Evolution** | ❌ Manual | ✅ Automatic |
| **Upserts** | ❌ Complex | ✅ Built-in MERGE |
| **Concurrent Writes** | ❌ Unsafe | ✅ Safe |
| **Audit Log** | ❌ No | ✅ Full history |
| **Rollback** | ❌ Impossible | ✅ Simple |

**Use Parquet when**: Simple read-only datasets, no updates needed

**Use Delta Lake when**: Production data lake, updates, compliance, governance

---

## 🎯 Next Steps

1. **Run the Pipeline**:
   ```bash
   mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.DataLakePipeline"
   ```

2. **Explore MinIO Console**: http://localhost:9001
   - Browse data-lake bucket
   - View Delta Lake files
   - Download Parquet files

3. **Query Your Data**:
   ```java
   spark.read()
       .format("delta")
       .load("s3a://data-lake/gold/loan_summary_by_purpose")
       .show();
   ```

4. **Customize for Your Use Case**:
   - Modify Bronze ingestion for your data sources
   - Add custom validation rules in Silver layer
   - Create business-specific aggregates in Gold layer

5. **Integrate with Existing Pipelines**:
   - Read from PostgreSQL → Write to Bronze
   - Read from Silver → Publish to Kafka
   - Read from Gold → Export to BI tools

---

## 📚 Additional Resources

- **Delta Lake Documentation**: https://docs.delta.io/
- **Delta Lake Quickstart**: https://docs.delta.io/latest/quick-start.html
- **Medallion Architecture**: https://www.databricks.com/glossary/medallion-architecture
- **MinIO Documentation**: https://min.io/docs/minio/linux/index.html
- **Spark + Delta Lake**: https://docs.delta.io/latest/api/python/spark/index.html

---

## 💡 Key Takeaways

1. **Delta Lake = Parquet + ACID + Time Travel + More**
2. **Medallion Architecture = Bronze (raw) → Silver (clean) → Gold (aggregates)**
3. **MinIO = Local S3 for testing**
4. **Time Travel = Query any historical version**
5. **MERGE = Efficient upserts (update or insert)**
6. **Schema Evolution = Add columns without rewriting**
7. **Optimize = Compact small files for performance**
8. **Vacuum = Clean up old versions**

---

**Happy Data Lake Building! 🏗️**

Your data is now queryable, version-controlled, and production-ready on MinIO!
