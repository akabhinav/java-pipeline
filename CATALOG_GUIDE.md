# Data Lake Catalog & Query Engine Guide

Complete guide for querying your Data Lake using **Trino** SQL query engine with **Hive Metastore** catalog.

---

## 🎯 What You Get

**Hive Metastore** (Catalog)
- Central metadata repository for all tables
- Stores table schemas, partitions, locations
- Backed by PostgreSQL for persistence
- Port: `9083` (Thrift interface)

**Trino** (Query Engine)
- Fast distributed SQL query engine
- Query Delta Lake tables with standard SQL
- No Spark code needed!
- Web UI: http://localhost:8085
- JDBC: `jdbc:trino://localhost:8085/delta/datalake`

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Trino Query Engine                       │
│                   (Port 8085 - SQL Interface)                │
│                                                              │
│  ┌──────────────────┐           ┌──────────────────┐       │
│  │ Hive Connector   │           │ Delta Connector  │       │
│  │ (General Tables) │           │ (Delta Tables)   │       │
│  └────────┬─────────┘           └────────┬─────────┘       │
└───────────┼────────────────────────────────┼────────────────┘
            │                                │
            └────────────┬───────────────────┘
                        │
                        ▼
         ┌──────────────────────────────┐
         │   Hive Metastore (Catalog)   │
         │    (Port 9083 - Metadata)    │
         │                              │
         │   ┌──────────────────────┐  │
         │   │  PostgreSQL Backend  │  │
         │   │  (metastore DB)      │  │
         │   └──────────────────────┘  │
         └──────────────┬───────────────┘
                        │
                        ▼
              ┌─────────────────┐
              │  MinIO (S3)     │
              │  Data Lake      │
              │                 │
              │  • bronze/      │
              │  • silver/      │
              │  • gold/        │
              └─────────────────┘
```

---

## 🚀 Quick Start

### Step 1: Start Full Environment

```bash
# Windows
start-local-environment.bat

# Mac/Linux
./start-local-environment.sh
```

This starts:
- MinIO (S3 storage)
- PostgreSQL (metastore backend + banking data)
- Kafka (streaming)
- Spark Cluster
- **Hive Metastore** (catalog) - NEW!
- **Trino** (query engine) - NEW!

### Step 2: Create Data Lake

```bash
# Windows
run-data-lake.bat

# Mac/Linux
./run-data-lake.sh
```

This creates Delta Lake tables in MinIO (bronze/silver/gold layers).

### Step 3: Connect to Trino

```bash
docker exec -it pipeline-trino trino --catalog delta --schema datalake
```

You'll see:
```
trino:datalake>
```

### Step 4: Run Your First Query

```sql
-- Show all tables
SHOW TABLES;

-- Query silver layer
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
```

---

## 📊 Available Tables

After running the Data Lake pipeline, these tables are available:

### Bronze Layer (Raw Data)

| Table | Description | Location |
|-------|-------------|----------|
| `loan_applications_bronze` | Raw loan application data | `s3a://data-lake/bronze/loan_applications` |
| `customers_bronze` | Raw customer data | `s3a://data-lake/bronze/customers` |

### Silver Layer (Cleaned Data)

| Table | Description | Location |
|-------|-------------|----------|
| `loan_applications_silver` | Cleaned loan applications | `s3a://data-lake/silver/loan_applications_clean` |
| `customers_silver` | Cleaned customer data | `s3a://data-lake/silver/customers_clean` |

### Gold Layer (Business Metrics)

| Table | Description | Location |
|-------|-------------|----------|
| `loan_summary_gold` | Loan aggregates by purpose | `s3a://data-lake/gold/loan_summary_by_purpose` |
| `customer_metrics_gold` | Customer statistics | `s3a://data-lake/gold/customer_metrics` |

---

## 🔍 Query Examples

### Basic Queries

```sql
-- Count records in each layer
SELECT 'Bronze' as layer, COUNT(*) as records FROM loan_applications_bronze
UNION ALL
SELECT 'Silver' as layer, COUNT(*) as records FROM loan_applications_silver
UNION ALL
SELECT 'Gold' as layer, COUNT(*) as records FROM loan_summary_gold;

-- View table schema
DESCRIBE loan_applications_silver;

-- Show table stats
SHOW STATS FOR loan_applications_silver;
```

### Filtering & Sorting

```sql
-- High-value loans
SELECT
    application_id,
    applicant_name,
    loan_amount,
    loan_purpose
FROM loan_applications_silver
WHERE loan_amount > 500000
ORDER BY loan_amount DESC;

-- Home loans only
SELECT * FROM loan_applications_silver
WHERE loan_purpose = 'HOME_LOAN';

-- Young applicants
SELECT * FROM loan_applications_silver
WHERE applicant_age < 30;
```

### Aggregations

```sql
-- Loans by purpose
SELECT
    loan_purpose,
    COUNT(*) as app_count,
    SUM(loan_amount) as total_amount,
    AVG(loan_amount) as avg_amount,
    MIN(loan_amount) as min_amount,
    MAX(loan_amount) as max_amount
FROM loan_applications_silver
GROUP BY loan_purpose
ORDER BY total_amount DESC;

-- Loans by age group
SELECT
    CASE
        WHEN applicant_age < 30 THEN '< 30'
        WHEN applicant_age BETWEEN 30 AND 40 THEN '30-40'
        WHEN applicant_age BETWEEN 41 AND 50 THEN '41-50'
        ELSE '> 50'
    END as age_group,
    COUNT(*) as loan_count,
    AVG(loan_amount) as avg_loan_amount
FROM loan_applications_silver
GROUP BY 1
ORDER BY 1;
```

### Joins

```sql
-- Enrich loans with customer data
SELECT
    l.application_id,
    l.applicant_name,
    l.loan_amount,
    l.loan_purpose,
    c.monthly_income,
    c.employment_status,
    ROUND(l.loan_amount / c.monthly_income, 2) as loan_to_income_ratio
FROM loan_applications_silver l
JOIN customers_silver c ON l.customer_id = c.customer_id
ORDER BY loan_to_income_ratio DESC;

-- Customers with multiple loans
SELECT
    c.customer_id,
    c.name,
    COUNT(l.application_id) as loan_count,
    SUM(l.loan_amount) as total_loan_amount
FROM customers_silver c
JOIN loan_applications_silver l ON c.customer_id = l.customer_id
GROUP BY c.customer_id, c.name
HAVING COUNT(l.application_id) > 1;
```

### Window Functions

```sql
-- Rank loans by amount within each purpose
SELECT
    application_id,
    loan_purpose,
    loan_amount,
    RANK() OVER (PARTITION BY loan_purpose ORDER BY loan_amount DESC) as rank
FROM loan_applications_silver
ORDER BY loan_purpose, rank;

-- Running total
SELECT
    application_id,
    loan_amount,
    SUM(loan_amount) OVER (
        ORDER BY application_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as running_total
FROM loan_applications_silver;
```

---

## 🌐 Trino Web UI

Access Trino Web UI at: **http://localhost:8085**

Features:
- View running queries
- Query history
- Cluster status
- Worker nodes
- Resource usage

---

## 🔧 Command Line Usage

### Connect to Trino

```bash
# Connect to default catalog
docker exec -it pipeline-trino trino

# Connect to delta catalog with schema
docker exec -it pipeline-trino trino --catalog delta --schema datalake

# Connect and execute query
docker exec -it pipeline-trino trino --execute "SELECT COUNT(*) FROM delta.datalake.loan_applications_silver"
```

### Trino CLI Commands

Inside Trino CLI:

```sql
-- Show catalogs
SHOW CATALOGS;

-- Show schemas in a catalog
SHOW SCHEMAS FROM delta;

-- Show tables in current schema
SHOW TABLES;

-- Switch catalog/schema
USE delta.datalake;

-- View query history
SELECT * FROM system.runtime.queries LIMIT 10;

-- Exit
quit;
```

---

## 🔗 JDBC Connection

Connect to Trino from applications using JDBC:

### Connection String

```
jdbc:trino://localhost:8085/delta/datalake
```

### Java Example

```java
import java.sql.*;

public class TrinoQuery {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:trino://localhost:8085/delta/datalake";

        try (Connection conn = DriverManager.getConnection(url)) {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "SELECT loan_purpose, COUNT(*) as count " +
                "FROM loan_applications_silver " +
                "GROUP BY loan_purpose"
            );

            while (rs.next()) {
                System.out.println(rs.getString("loan_purpose") + ": " +
                                   rs.getInt("count"));
            }
        }
    }
}
```

### Python Example

```python
from trino.dbapi import connect

conn = connect(
    host='localhost',
    port=8085,
    catalog='delta',
    schema='datalake'
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM loan_applications_silver LIMIT 10")
rows = cursor.fetchall()

for row in rows:
    print(row)
```

---

## 📈 Creating Tables in Catalog

### Option 1: Register Existing Delta Tables

```sql
-- Register existing Delta Lake table
CREATE TABLE delta.datalake.my_table (
    id VARCHAR,
    name VARCHAR,
    amount DOUBLE
)
WITH (
    location = 's3a://data-lake/my-data/',
    format = 'DELTA'
);
```

### Option 2: Create New Table

```sql
-- Create new table (writes to MinIO)
CREATE TABLE delta.datalake.new_table (
    id VARCHAR,
    value DOUBLE,
    created_at TIMESTAMP
)
WITH (
    location = 's3a://data-lake/new-table/',
    format = 'DELTA'
);

-- Insert data
INSERT INTO delta.datalake.new_table
VALUES ('1', 100.50, CURRENT_TIMESTAMP);
```

### Option 3: Create Table from Query (CTAS)

```sql
-- Create table from existing data
CREATE TABLE delta.datalake.high_value_loans
WITH (
    location = 's3a://data-lake/high-value-loans/',
    format = 'DELTA'
)
AS
SELECT *
FROM loan_applications_silver
WHERE loan_amount > 500000;
```

---

## 🛠️ Administration

### View Catalog Metadata

```sql
-- Show all tables
SELECT * FROM delta.information_schema.tables;

-- Show columns
SELECT * FROM delta.information_schema.columns
WHERE table_name = 'loan_applications_silver';

-- Show table properties
SHOW CREATE TABLE loan_applications_silver;
```

### Drop Tables

```sql
-- Drop table (keeps data in MinIO)
DROP TABLE delta.datalake.loan_applications_silver;

-- Drop schema
DROP SCHEMA delta.datalake;
```

### Refresh Metadata

If you add data outside of Trino (e.g., via Spark):

```sql
-- Refresh table metadata
CALL delta.system.sync_partition_metadata('datalake', 'loan_applications_silver');
```

---

## 🐛 Troubleshooting

### Issue 1: "Catalog delta not found"

**Solution**: Check Trino is running
```bash
docker ps | grep trino
docker logs pipeline-trino
```

### Issue 2: "Schema datalake does not exist"

**Solution**: Create schema first
```sql
CREATE SCHEMA IF NOT EXISTS delta.datalake
WITH (location = 's3a://data-lake/');
```

### Issue 3: "Table not found"

**Solution**: Register the table (see Creating Tables section)

### Issue 4: Connection Refused

**Solution**: Check all services are running
```bash
docker-compose ps

# Should show:
# - pipeline-trino (healthy)
# - pipeline-hive-metastore (healthy)
# - pipeline-minio (healthy)
# - pipeline-postgres (healthy)
```

### Issue 5: S3 Access Denied

**Solution**: Check MinIO credentials in `/docker/trino/etc/catalog/delta.properties`:
```properties
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
```

---

## ⚡ Performance Tips

### 1. Use Partitioned Tables

```sql
CREATE TABLE delta.datalake.loans_partitioned (
    application_id VARCHAR,
    loan_amount DOUBLE,
    loan_purpose VARCHAR,
    application_date DATE
)
WITH (
    location = 's3a://data-lake/loans-partitioned/',
    format = 'DELTA',
    partitioned_by = ARRAY['loan_purpose', 'application_date']
);
```

**Benefit**: Queries filtering on `loan_purpose` or `application_date` only scan relevant partitions!

### 2. Use EXPLAIN to Understand Queries

```sql
EXPLAIN SELECT * FROM loan_applications_silver WHERE loan_amount > 500000;

-- Even better: See actual execution stats
EXPLAIN ANALYZE
SELECT loan_purpose, COUNT(*)
FROM loan_applications_silver
GROUP BY loan_purpose;
```

### 3. Limit Columns in SELECT

```sql
-- Slow: Reads all columns
SELECT * FROM loan_applications_silver;

-- Fast: Only reads needed columns
SELECT application_id, loan_amount FROM loan_applications_silver;
```

### 4. Use WHERE Before JOIN

```sql
-- Better: Filter before join
SELECT l.*, c.*
FROM (SELECT * FROM loan_applications_silver WHERE loan_amount > 500000) l
JOIN customers_silver c ON l.customer_id = c.customer_id;
```

---

## 📚 Additional Query Examples

All examples are in `docker/query-examples.sql`. Run them:

```bash
# Connect to Trino
docker exec -it pipeline-trino trino --catalog delta --schema datalake

# Copy/paste queries from query-examples.sql
```

---

## 🎯 Common Use Cases

### Use Case 1: Daily Analytics Dashboard

```sql
-- KPIs for today
SELECT
    COUNT(*) as total_applications,
    SUM(loan_amount) as total_requested,
    AVG(loan_amount) as avg_loan_amount,
    MAX(loan_amount) as largest_loan
FROM loan_applications_bronze
WHERE DATE(ingestion_timestamp) = CURRENT_DATE;
```

### Use Case 2: Risk Analysis

```sql
-- Loan-to-income ratio analysis
SELECT
    CASE
        WHEN loan_amount / monthly_income < 3 THEN 'Low Risk'
        WHEN loan_amount / monthly_income BETWEEN 3 AND 5 THEN 'Medium Risk'
        ELSE 'High Risk'
    END as risk_category,
    COUNT(*) as application_count,
    AVG(loan_amount) as avg_loan_amount
FROM loan_applications_silver
GROUP BY 1
ORDER BY application_count DESC;
```

### Use Case 3: Customer Segmentation

```sql
-- Customer segments by income and employment
SELECT
    employment_status,
    CASE
        WHEN monthly_income < 50000 THEN 'Low Income'
        WHEN monthly_income BETWEEN 50000 AND 100000 THEN 'Medium Income'
        ELSE 'High Income'
    END as income_segment,
    COUNT(*) as customer_count
FROM customers_silver
GROUP BY employment_status, 2
ORDER BY employment_status, income_segment;
```

---

## 🔄 Integration with BI Tools

### Connect from Tableau

1. Install Trino JDBC driver
2. Connection type: "Other Databases (JDBC)"
3. URL: `jdbc:trino://localhost:8085/delta/datalake`
4. Driver class: `io.trino.jdbc.TrinoDriver`

### Connect from Power BI

1. Use "ODBC" connector
2. Install Trino ODBC driver
3. DSN: Point to `localhost:8085`
4. Catalog: `delta`, Schema: `datalake`

### Connect from Superset

```python
SQLALCHEMY_DATABASE_URI = 'trino://localhost:8085/delta/datalake'
```

---

## 🎓 Learning Resources

- **Trino Documentation**: https://trino.io/docs/current/
- **Delta Lake + Trino**: https://trino.io/docs/current/connector/delta-lake.html
- **Hive Metastore**: https://cwiki.apache.org/confluence/display/Hive/AdminManual+Metastore+Administration
- **SQL Queries**: `docker/query-examples.sql` (70+ examples)

---

## 📋 Quick Reference

### Ports

| Service | Port | URL |
|---------|------|-----|
| Trino UI | 8085 | http://localhost:8085 |
| Hive Metastore | 9083 | thrift://localhost:9083 |
| MinIO Console | 9001 | http://localhost:9001 |
| PostgreSQL | 5432 | localhost:5432 |

### Commands

```bash
# Connect to Trino
docker exec -it pipeline-trino trino --catalog delta --schema datalake

# View Trino logs
docker logs pipeline-trino -f

# View Metastore logs
docker logs pipeline-hive-metastore -f

# Restart Trino
docker-compose restart trino

# Check service health
docker-compose ps
```

### SQL Basics

```sql
SHOW CATALOGS;                          -- List catalogs
SHOW SCHEMAS FROM delta;                -- List schemas
SHOW TABLES FROM delta.datalake;        -- List tables
DESCRIBE delta.datalake.table_name;     -- Table schema
SELECT * FROM table_name LIMIT 10;      -- Query table
```

---

## 🎉 Summary

You now have:

✅ **Hive Metastore** - Central catalog for all table metadata
✅ **Trino** - Fast SQL query engine (no Spark code needed!)
✅ **Delta Lake Support** - Query Delta tables with SQL
✅ **Web UI** - Visual query interface at http://localhost:8085
✅ **JDBC/ODBC** - Connect from any BI tool
✅ **70+ Query Examples** - Ready-to-use SQL queries

**Next Steps:**
1. Run Data Lake pipeline: `./run-data-lake.sh`
2. Connect to Trino: `docker exec -it pipeline-trino trino --catalog delta --schema datalake`
3. Start querying: `SELECT * FROM loan_applications_silver LIMIT 10;`

**Happy Querying! 🎯**
