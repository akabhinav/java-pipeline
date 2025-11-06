# How to Query Data from MinIO (Local S3)

Complete guide for querying data stored in MinIO using Spark on your local system.

---

## 🎯 Overview

MinIO is **S3-compatible object storage** running locally. You can query data from MinIO exactly like you would query from AWS S3!

**MinIO Setup:**
- Endpoint: `http://localhost:9000`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Console: `http://localhost:9001`

---

## ⚙️ Spark Configuration for MinIO

### Configure Spark Session

```java
import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession.builder()
    .appName("Query MinIO Data")
    .master("local[*]")

    // MinIO (S3) Configuration
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    // Optional: Connection settings
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")

    .getOrCreate();
```

**Key Points:**
- Use `s3a://` protocol (not `s3://`)
- Set `path.style.access` to `true` for MinIO
- Endpoint is `http://localhost:9000` (not HTTPS for local)

---

## 📊 Query CSV Files from MinIO

### Example: Read CSV

```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// Read CSV from MinIO bucket "banking-data"
Dataset<Row> customers = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("s3a://banking-data/customers/customers.csv");

// Show data
customers.show();

// Query with SQL
customers.createOrReplaceTempView("customers");
Dataset<Row> highIncome = spark.sql(
    "SELECT * FROM customers WHERE monthly_income > 100000"
);
highIncome.show();
```

### Available CSV Files in MinIO

After running `./start-local-environment.sh`:

| Bucket | Path | Description |
|--------|------|-------------|
| banking-data | `s3a://banking-data/customers/customers.csv` | Customer data (15 records) |
| banking-data | `s3a://banking-data/credit-bureau/credit_bureau.csv` | Credit bureau data (15 records) |

---

## 🗄️ Query Parquet Files from MinIO

### Example: Read Parquet

```java
// Write Parquet to MinIO
customers.write()
    .mode("overwrite")
    .parquet("s3a://banking-data/customers-parquet/");

// Read Parquet from MinIO
Dataset<Row> customersParquet = spark.read()
    .parquet("s3a://banking-data/customers-parquet/");

customersParquet.show();
```

---

## ⚡ Query Delta Lake Tables from MinIO

### Example: Read Delta Lake

```java
import io.delta.tables.DeltaTable;

// Configure Spark with Delta Lake support
SparkSession spark = SparkSession.builder()
    .appName("Query Delta Lake from MinIO")
    .master("local[*]")

    // Delta Lake extensions
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    // MinIO configuration
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    .getOrCreate();

// Read Delta table from MinIO
Dataset<Row> loansSilver = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean");

loansSilver.show();
```

### Available Delta Lake Tables

After running `./run-data-lake.sh`:

| Layer | Path | Description |
|-------|------|-------------|
| Bronze | `s3a://data-lake/bronze/loan_applications` | Raw loan data |
| Bronze | `s3a://data-lake/bronze/customers` | Raw customer data |
| Silver | `s3a://data-lake/silver/loan_applications_clean` | Cleaned loans |
| Silver | `s3a://data-lake/silver/customers_clean` | Cleaned customers |
| Gold | `s3a://data-lake/gold/loan_summary_by_purpose` | Loan aggregates |
| Gold | `s3a://data-lake/gold/customer_metrics` | Customer metrics |

---

## 🔍 Advanced Queries

### Query with Filters

```java
// Read and filter in one step
Dataset<Row> highValueLoans = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean")
    .filter("loan_amount > 500000")
    .select("application_id", "applicant_name", "loan_amount");

highValueLoans.show();
```

### Aggregate Queries

```java
import static org.apache.spark.sql.functions.*;

Dataset<Row> loans = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean");

// Group by loan purpose
Dataset<Row> summary = loans.groupBy("loan_purpose")
    .agg(
        count("*").as("total_applications"),
        sum("loan_amount").as("total_amount"),
        avg("loan_amount").as("avg_amount"),
        max("loan_amount").as("max_amount"),
        min("loan_amount").as("min_amount")
    )
    .orderBy(desc("total_amount"));

summary.show();
```

Output:
```
+---------------+------------------+------------+-----------+-----------+-----------+
|  loan_purpose |total_applications|total_amount|avg_amount |max_amount |min_amount |
+---------------+------------------+------------+-----------+-----------+-----------+
|BUSINESS_LOAN  |1                 |2000000.0   |2000000.0  |2000000.0  |2000000.0  |
|HOME_LOAN      |1                 |500000.0    |500000.0   |500000.0   |500000.0   |
|PERSONAL_LOAN  |2                 |450000.0    |225000.0   |300000.0   |150000.0   |
|CAR_LOAN       |1                 |800000.0    |800000.0   |800000.0   |800000.0   |
+---------------+------------------+------------+-----------+-----------+-----------+
```

### Join Multiple Tables

```java
Dataset<Row> loans = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean");

Dataset<Row> customers = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/customers_clean");

// Join loans with customer data
Dataset<Row> enriched = loans.join(
    customers,
    loans.col("customer_id").equalTo(customers.col("customer_id")),
    "inner"
).select(
    loans.col("application_id"),
    loans.col("applicant_name"),
    loans.col("loan_amount"),
    loans.col("loan_purpose"),
    customers.col("monthly_income"),
    customers.col("employment_status")
);

enriched.show();
```

---

## 🕐 Time Travel Queries

### Query Historical Versions

```java
// Query version 0 (initial load)
Dataset<Row> version0 = spark.read()
    .format("delta")
    .option("versionAsOf", "0")
    .load("s3a://data-lake/silver/loan_applications_clean");

System.out.println("Version 0 record count: " + version0.count());

// Query as of specific timestamp
Dataset<Row> yesterday = spark.read()
    .format("delta")
    .option("timestampAsOf", "2025-11-05 10:00:00")
    .load("s3a://data-lake/silver/loan_applications_clean");

System.out.println("Yesterday's record count: " + yesterday.count());
```

### View Table History

```java
import io.delta.tables.DeltaTable;

DeltaTable deltaTable = DeltaTable.forPath(spark,
    "s3a://data-lake/silver/loan_applications_clean");

// Show last 10 operations
deltaTable.history(10)
    .select("version", "timestamp", "operation", "operationMetrics")
    .show(10, false);
```

Output:
```
+-------+-------------------+----------+---------------------------+
|version|timestamp          |operation |operationMetrics           |
+-------+-------------------+----------+---------------------------+
|2      |2025-11-06 10:35:00|MERGE     |{numTargetRowsUpdated=5}   |
|1      |2025-11-06 10:32:00|WRITE     |{numFiles=2, numRows=10}   |
|0      |2025-11-06 10:30:00|WRITE     |{numFiles=1, numRows=5}    |
+-------+-------------------+----------+---------------------------+
```

---

## 🔧 Complete Working Example

Create a new file: `QueryMinIOExample.java`

```java
package com.enterprise.pipeline.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import io.delta.tables.DeltaTable;

import static org.apache.spark.sql.functions.*;

public class QueryMinIOExample {

    public static void main(String[] args) {
        // 1. Create Spark session with MinIO configuration
        SparkSession spark = SparkSession.builder()
            .appName("Query MinIO Data")
            .master("local[*]")

            // Delta Lake
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")

            // MinIO
            .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

            .getOrCreate();

        try {
            System.out.println("=====================================");
            System.out.println("  Querying Data from MinIO");
            System.out.println("=====================================\n");

            // 2. Query CSV files
            System.out.println("1. Reading CSV from MinIO...");
            Dataset<Row> customers = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("s3a://banking-data/customers/customers.csv");

            System.out.println("Total customers: " + customers.count());
            customers.show(5);

            // 3. Query Delta Lake tables
            System.out.println("\n2. Reading Delta Lake from MinIO...");
            Dataset<Row> loans = spark.read()
                .format("delta")
                .load("s3a://data-lake/silver/loan_applications_clean");

            System.out.println("Total loans: " + loans.count());
            loans.show(5);

            // 4. Aggregate queries
            System.out.println("\n3. Aggregate Query - Loans by Purpose:");
            Dataset<Row> summary = loans.groupBy("loan_purpose")
                .agg(
                    count("*").as("total_apps"),
                    sum("loan_amount").as("total_amount"),
                    avg("loan_amount").as("avg_amount")
                )
                .orderBy(desc("total_amount"));

            summary.show();

            // 5. Join tables
            System.out.println("\n4. Join Query - Enriched Loan Data:");
            Dataset<Row> customersClean = spark.read()
                .format("delta")
                .load("s3a://data-lake/silver/customers_clean");

            Dataset<Row> enriched = loans.join(
                customersClean,
                loans.col("customer_id").equalTo(customersClean.col("customer_id"))
            ).select(
                loans.col("application_id"),
                loans.col("loan_amount"),
                loans.col("loan_purpose"),
                customersClean.col("monthly_income"),
                customersClean.col("employment_status")
            );

            enriched.show();

            // 6. SQL queries
            System.out.println("\n5. SQL Query:");
            loans.createOrReplaceTempView("loans");
            Dataset<Row> highValue = spark.sql(
                "SELECT application_id, applicant_name, loan_amount, loan_purpose " +
                "FROM loans " +
                "WHERE loan_amount > 500000 " +
                "ORDER BY loan_amount DESC"
            );
            highValue.show();

            // 7. Time travel
            System.out.println("\n6. Time Travel - Query Version 0:");
            Dataset<Row> version0 = spark.read()
                .format("delta")
                .option("versionAsOf", "0")
                .load("s3a://data-lake/silver/loan_applications_clean");

            System.out.println("Version 0 count: " + version0.count());

            // 8. Table history
            System.out.println("\n7. Delta Table History:");
            DeltaTable deltaTable = DeltaTable.forPath(spark,
                "s3a://data-lake/silver/loan_applications_clean");

            deltaTable.history(5)
                .select("version", "timestamp", "operation")
                .show(5, false);

            System.out.println("\n=====================================");
            System.out.println("  ✅ All Queries Completed!");
            System.out.println("=====================================");

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            spark.stop();
        }
    }
}
```

### Run the Example

```bash
# 1. Ensure environment is running
./start-local-environment.sh

# 2. Run Data Lake pipeline first (to create data)
./run-data-lake.sh

# 3. Run the query example
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.QueryMinIOExample"
```

---

## 🎯 Common Use Cases

### Use Case 1: Daily Data Analysis

```java
// Read today's loan applications
Dataset<Row> todayLoans = spark.read()
    .format("delta")
    .load("s3a://data-lake/bronze/loan_applications")
    .filter(date_format(col("ingestion_timestamp"), "yyyy-MM-dd")
            .equalTo(current_date()));

// Analyze by purpose
todayLoans.groupBy("loan_purpose")
    .count()
    .show();
```

### Use Case 2: Historical Comparison

```java
// Compare this week vs last week
Dataset<Row> thisWeek = spark.read()
    .format("delta")
    .option("timestampAsOf", "2025-11-06 00:00:00")
    .load("s3a://data-lake/gold/loan_summary_by_purpose");

Dataset<Row> lastWeek = spark.read()
    .format("delta")
    .option("timestampAsOf", "2025-10-30 00:00:00")
    .load("s3a://data-lake/gold/loan_summary_by_purpose");

Dataset<Row> comparison = thisWeek.join(
    lastWeek,
    thisWeek.col("loan_purpose").equalTo(lastWeek.col("loan_purpose"))
).select(
    thisWeek.col("loan_purpose"),
    thisWeek.col("total_amount").as("this_week"),
    lastWeek.col("total_amount").as("last_week"),
    (thisWeek.col("total_amount").minus(lastWeek.col("total_amount")))
        .as("change")
);

comparison.show();
```

### Use Case 3: Export to CSV

```java
// Query and export results
Dataset<Row> report = spark.read()
    .format("delta")
    .load("s3a://data-lake/gold/loan_summary_by_purpose");

// Save back to MinIO as CSV
report.coalesce(1)
    .write()
    .option("header", "true")
    .mode("overwrite")
    .csv("s3a://reports/loan-summary-" +
         java.time.LocalDate.now() + ".csv");
```

---

## 🐛 Troubleshooting

### Issue 1: Connection Refused

**Error**: `Connection refused to localhost:9000`

**Solution**:
```bash
# Check MinIO is running
docker ps | grep minio

# If not running, start environment
./start-local-environment.sh
```

### Issue 2: Access Denied

**Error**: `Access Denied (Service: Amazon S3)`

**Solution**: Check credentials in Spark config
```java
.config("spark.hadoop.fs.s3a.access.key", "minioadmin")
.config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
```

### Issue 3: Path Not Found

**Error**: `Path does not exist: s3a://data-lake/...`

**Solution**: Create data first
```bash
# Run Data Lake pipeline to create data
./run-data-lake.sh
```

### Issue 4: Slow Queries

**Optimization**: Enable S3A committers
```java
.config("spark.hadoop.fs.s3a.committer.name", "magic")
.config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
```

---

## 📊 Performance Tips

### 1. Partition Filtering

```java
// Efficient: Only reads HOME_LOAN partition
Dataset<Row> homeLoans = spark.read()
    .format("delta")
    .load("s3a://data-lake/bronze/loan_applications")
    .filter("loan_purpose = 'HOME_LOAN'");  // Partition column
```

### 2. Column Pruning

```java
// Only read needed columns
Dataset<Row> selected = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean")
    .select("application_id", "loan_amount");  // Don't read all columns
```

### 3. Caching for Repeated Queries

```java
Dataset<Row> loans = spark.read()
    .format("delta")
    .load("s3a://data-lake/silver/loan_applications_clean");

// Cache for multiple queries
loans.cache();

// Run multiple queries
loans.filter("loan_amount > 500000").count();
loans.groupBy("loan_purpose").count().show();
loans.filter("applicant_age < 30").show();

// Unpersist when done
loans.unpersist();
```

---

## 🎓 Next Steps

1. **Explore MinIO Console**: http://localhost:9001
   - Browse buckets visually
   - Upload/download files
   - View Delta Lake structure

2. **Run Example Pipelines**:
   - `./run-data-lake.sh` - Create Data Lake
   - `./run-pipeline-on-cluster.sh RealConnectorsPipeline` - Full pipeline

3. **Read Documentation**:
   - [DATA_LAKE_GUIDE.md](DATA_LAKE_GUIDE.md) - Complete Data Lake guide
   - [LOCAL_TESTING_GUIDE.md](LOCAL_TESTING_GUIDE.md) - Local environment guide

4. **Customize for Your Data**:
   - Upload your CSV/Parquet files to MinIO
   - Query them using the examples above
   - Build custom pipelines

---

## 📚 Quick Reference

### S3A Paths

| Format | Example |
|--------|---------|
| CSV | `s3a://bucket-name/path/file.csv` |
| Parquet | `s3a://bucket-name/path/` |
| Delta | `s3a://bucket-name/path/` |

### Read Formats

```java
// CSV
spark.read().option("header", "true").csv("s3a://...");

// Parquet
spark.read().parquet("s3a://...");

// Delta
spark.read().format("delta").load("s3a://...");

// JSON
spark.read().option("multiline", "true").json("s3a://...");
```

### Write Modes

```java
.mode("overwrite")   // Replace existing data
.mode("append")      // Add to existing data
.mode("ignore")      // Skip if exists
.mode("error")       // Fail if exists (default)
```

---

**Happy Querying! 🎯**

Your MinIO data is now fully queryable with Spark!
