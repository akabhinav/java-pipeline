# Windows Deployment Guide - Enterprise Pipeline Platform

## Complete Guide to Running Spark Pipeline Jobs on Windows

This guide provides step-by-step instructions for running the Enterprise Pipeline Platform on Windows with a local Spark cluster using Docker.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Detailed Setup Instructions](#detailed-setup-instructions)
4. [Submitting Jobs to Spark Cluster](#submitting-jobs-to-spark-cluster)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)
7. [Architecture Overview](#architecture-overview)

---

## Prerequisites

### Required Software

1. **Docker Desktop for Windows**
   - Version: 20.10 or later
   - Download: https://www.docker.com/products/docker-desktop
   - Ensure WSL 2 backend is enabled
   - Minimum 8GB RAM allocated to Docker

2. **Java Development Kit (JDK)**
   - Version: JDK 11 or later
   - Download: https://adoptium.net/
   - Set `JAVA_HOME` environment variable

3. **Apache Maven**
   - Version: 3.6 or later
   - Download: https://maven.apache.org/download.cgi
   - Add to PATH environment variable

4. **Git for Windows**
   - Download: https://git-scm.com/download/win

### System Requirements

- Windows 10/11 (64-bit)
- Minimum 16GB RAM (8GB for Docker, 8GB for system)
- At least 20GB free disk space
- Internet connection for downloading dependencies

---

## Quick Start

### 1. Start the Environment

Open Command Prompt or PowerShell and navigate to the project directory:

```cmd
cd D:\Project\java-pipeline
```

Start all services (PostgreSQL, MinIO, Kafka, Spark Cluster):

```cmd
start-local-environment.bat
```

**Expected Output:**
```
================================================================================
  Starting Enterprise Pipeline Platform - Local Environment
================================================================================

[OK] Docker is running
[OK] Network: pipeline-network created
[OK] Starting PostgreSQL...
[OK] Starting MinIO (S3)...
[OK] Starting Kafka & Zookeeper...
[OK] Starting Spark Master...
[OK] Starting Spark Workers (2)...
[OK] Starting Kafka UI...
[OK] Starting pgAdmin...

All services started successfully!

Services Available:
  Spark Master UI:    http://localhost:8081
  Spark Worker 1:     http://localhost:8082
  Spark Worker 2:     http://localhost:8083
  MinIO Console:      http://localhost:9003
  Kafka UI:           http://localhost:8084
  pgAdmin:            http://localhost:5050
  PostgreSQL:         localhost:5434
```

**Wait Time:** 2-3 minutes for all services to be fully ready.

### 2. Build the Project (First Time Only)

```cmd
mvn clean package -DskipTests
```

**Expected Output:**
```
[INFO] BUILD SUCCESS
[INFO] Total time:  45.123 s
```

### 3. Submit Job to Spark Cluster

```cmd
run-pipeline-on-cluster.bat RealConnectorsPipeline
```

**Expected Output:**
```
================================================================================
  Running Pipeline on Spark Cluster
================================================================================

[OK] Spark cluster is running
[OK] JAR file found: pipeline-examples\target\pipeline-examples-1.0.0-SNAPSHOT.jar

Submitting job to Spark cluster...
Pipeline Class: com.enterprise.pipeline.examples.RealConnectorsPipeline
Spark Master:   spark://localhost:7077

🚀 Real Connectors Pipeline - Starting
📊 STEP 1: Loading data from PostgreSQL...
  ✓ Loaded 15 loan applications
  ✓ Loaded 15 customers
  ✓ Loaded 30 transactions

✅ Pipeline Completed Successfully!
```

---

## Detailed Setup Instructions

### Step 1: Clone and Verify Project Structure

```cmd
cd D:\Project\java-pipeline
dir
```

You should see:
```
docker-compose.yml
start-local-environment.bat
run-pipeline-on-cluster.bat
pom.xml
pipeline-sdk\
pipeline-examples\
```

### Step 2: Configure Docker Desktop

1. Open Docker Desktop
2. Go to **Settings** → **Resources**
3. Set Memory to at least **8GB**
4. Set CPUs to at least **4**
5. Click **Apply & Restart**

### Step 3: Start Infrastructure Services

The `start-local-environment.bat` script starts:

| Service | Purpose | Port | UI URL |
|---------|---------|------|--------|
| PostgreSQL | Database for loan data | 5434 | - |
| MinIO | S3-compatible storage | 9002, 9003 | http://localhost:9003 |
| Kafka + Zookeeper | Message streaming | 9092, 9094 | - |
| Kafka UI | Kafka monitoring | 8084 | http://localhost:8084 |
| pgAdmin | PostgreSQL UI | 5050 | http://localhost:5050 |
| Spark Master | Cluster coordinator | 7077, 8081 | http://localhost:8081 |
| Spark Worker 1 | Executor node | 8082 | http://localhost:8082 |
| Spark Worker 2 | Executor node | 8083 | http://localhost:8083 |

**Note:** Ports have been adjusted to avoid conflicts with local services:
- PostgreSQL: 5434 (instead of 5432)
- MinIO: 9002/9003 (instead of 9000/9001)
- Kafka UI: 8084 (instead of 8080)

### Step 4: Verify Services Are Running

```cmd
docker ps
```

Expected containers:
```
pipeline-spark-master
pipeline-spark-worker-1
pipeline-spark-worker-2
pipeline-postgres
pipeline-minio
pipeline-kafka
pipeline-zookeeper
pipeline-kafka-ui
pipeline-pgadmin
```

Check Spark Master UI:
```cmd
start http://localhost:8081
```

You should see:
- Workers: 2
- Status: ALIVE
- Cores: 2 (1 per worker)
- Memory: 2GB total

---

## Submitting Jobs to Spark Cluster

### Basic Submission

```cmd
run-pipeline-on-cluster.bat [PipelineClassName]
```

### Available Pipelines

1. **RealConnectorsPipeline** (Recommended for testing)
   ```cmd
   run-pipeline-on-cluster.bat RealConnectorsPipeline
   ```
   - Loads data from PostgreSQL
   - Enriches with S3 credit bureau data
   - Applies rule engine (validation, transformation, business rules)
   - Saves results to PostgreSQL
   - Publishes to Kafka
   - Uploads to MinIO/S3

2. **ComprehensiveBankingPipeline**
   ```cmd
   run-pipeline-on-cluster.bat ComprehensiveBankingPipeline
   ```
   - Full banking loan processing pipeline
   - Multiple rule types and transformations

### Understanding the Submission Script

The `run-pipeline-on-cluster.bat` script performs:

1. **Pre-flight checks:**
   - Verifies Spark Master is running
   - Verifies Spark Workers are available
   - Checks if JAR file exists (builds if missing)

2. **Job submission via spark-submit:**
   ```cmd
   docker exec pipeline-spark-master spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       --class com.enterprise.pipeline.examples.RealConnectorsPipeline \
       --driver-memory 1g \
       --executor-memory 1g \
       --executor-cores 1 \
       --num-executors 2 \
       --conf spark.sql.adaptive.enabled=true \
       --conf spark.dynamicAllocation.enabled=false \
       --packages org.apache.hadoop:hadoop-aws:3.3.4,... \
       /opt/spark-apps/pipeline-examples-1.0.0-SNAPSHOT.jar
   ```

3. **Key Configuration:**
   - **Master URL:** `spark://spark-master:7077` (cluster mode)
   - **Deploy Mode:** `client` (driver runs in submit container)
   - **Executors:** 2 (one per worker)
   - **Memory:** 1GB per executor, 1GB driver
   - **Packages:** Hadoop AWS (S3), Kafka, PostgreSQL JDBC

### Critical Configuration: SparkSession

**IMPORTANT:** The SparkSession in your pipeline code must NOT have `.master()` hardcoded:

**CORRECT (Pipeline code):**
```java
SparkSession spark = SparkSession.builder()
    .appName("Real Connectors Pipeline")
    // NO .master() here - respects spark-submit configuration
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .getOrCreate();
```

**WRONG (Will force local mode):**
```java
SparkSession spark = SparkSession.builder()
    .appName("Real Connectors Pipeline")
    .master("local[*]")  // ❌ DO NOT DO THIS - overrides spark-submit!
    .config(...)
    .getOrCreate();
```

If `.master("local[*]")` is present, your job will:
- Run in local mode (single JVM)
- NOT appear in Spark Master UI
- NOT distribute work to workers
- Ignore `--master` from spark-submit

---

## Verification

### 1. Check Spark Master UI

Open http://localhost:8081 in your browser.

**During Job Execution:**
- **Running Applications:** Should show 1
- **Application Name:** Real Connectors Pipeline
- **Cores in Use:** 2
- **Memory in Use:** 2GB

**After Job Completion:**
- **Completed Applications:** Should show 1
- Click on application to see:
  - Executors: 4 total (2 per worker)
  - Tasks distributed across all executors
  - Stages completed successfully

### 2. Check Worker UIs

**Worker 1:** http://localhost:8082
```
Running Executors: 2
Executor IDs: app-xxx-0000/2, app-xxx-0000/3
```

**Worker 2:** http://localhost:8083
```
Running Executors: 2
Executor IDs: app-xxx-0000/0, app-xxx-0000/1
```

### 3. Verify Data in PostgreSQL

```cmd
docker exec -it pipeline-postgres psql -U pipeline -d banking
```

```sql
-- Check loaded data
SELECT COUNT(*) FROM loan_applications;  -- Should return 15
SELECT COUNT(*) FROM customers;          -- Should return 15
SELECT COUNT(*) FROM transactions;       -- Should return 30

-- Check pipeline results
SELECT COUNT(*) FROM loan_approval_results;     -- Pipeline output
SELECT COUNT(*) FROM fraud_detection_results;   -- Fraud analysis

-- Exit
\q
```

### 4. Verify Data in MinIO (S3)

1. Open http://localhost:9003
2. Login:
   - Username: `minioadmin`
   - Password: `minioadmin`
3. Check buckets:
   - `banking-data` → should contain `credit-bureau/credit_bureau.csv`
   - `pipeline-output` → should contain `loan-approval-results/` folder

### 5. Verify Kafka Messages

1. Open http://localhost:8084
2. Go to **Topics**
3. Find `loan-approval-results`
4. Click **Messages**
5. You should see JSON messages with loan approval data

### 6. Command Line Verification

```cmd
REM Check running containers
docker ps

REM Check Spark Master logs
docker logs pipeline-spark-master --tail 50

REM Check Worker 1 logs
docker logs pipeline-spark-worker-1 --tail 50

REM Quick Spark UI check
curl -s http://localhost:8081 | findstr "Applications"
```

---

## Troubleshooting

### Issue 1: "Port already in use"

**Error:**
```
Error response from daemon: Ports are not available: exposing port TCP 0.0.0.0:5432
```

**Solution:**
Port conflicts with local services. The docker-compose.yml has been configured to use alternate ports:
- PostgreSQL: 5434 (instead of 5432)
- MinIO: 9002/9003 (instead of 9000/9001)
- Kafka UI: 8084 (instead of 8080)

If you still have conflicts, stop local services:
```cmd
net stop postgresql-x64-14
net stop minio
```

Or modify ports in `docker-compose.yml`.

### Issue 2: "Spark Master not running"

**Error:**
```
[ERROR] Spark Master not running!
```

**Solution:**
```cmd
REM Check Docker Desktop is running
docker ps

REM Restart the environment
docker-compose down
start-local-environment.bat

REM Wait 2-3 minutes for services to initialize
```

### Issue 3: "JAR signature validation failed"

**Error:**
```
Invalid signature file digest for Manifest main attributes
```

**Solution:**
Already fixed in `pipeline-examples/pom.xml`:
```xml
<filters>
    <filter>
        <artifact>*:*</artifact>
        <excludes>
            <exclude>META-INF/*.SF</exclude>
            <exclude>META-INF/*.DSA</exclude>
            <exclude>META-INF/*.RSA</exclude>
        </excludes>
    </filter>
</filters>
```

Rebuild:
```cmd
mvn clean package -DskipTests
```

### Issue 4: "Connection refused - localhost:5432"

**Error:**
```
Connection to localhost:5432 refused
```

**Cause:** Code is using `localhost` instead of Docker service names.

**Solution:**
In pipeline code, use Docker service names:

```java
// ❌ WRONG - doesn't work in Docker network
private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/banking";
private static final String S3_ENDPOINT = "http://localhost:9000";
private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";

// ✅ CORRECT - uses Docker service names
private static final String POSTGRES_URL = "jdbc:postgresql://postgres:5432/banking";
private static final String S3_ENDPOINT = "http://minio:9000";
private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka:29092";
```

**Note:** Port mapping is only for external access from Windows. Inside Docker network, use internal ports:
- PostgreSQL: `postgres:5432` (not 5434)
- MinIO: `minio:9000` (not 9002)
- Kafka: `kafka:29092` (not 9092)

### Issue 5: "PATH_NOT_FOUND - s3a://banking-data/credit-bureau/credit_bureau.csv"

**Error:**
```
[PATH_NOT_FOUND] Path does not exist: s3a://banking-data/credit-bureau/credit_bureau.csv
```

**Solution:**
Upload credit bureau data to MinIO:

```cmd
REM Install MinIO client
docker exec pipeline-minio mkdir -p /data/banking-data/credit-bureau

REM Copy file from project
docker cp credit_bureau.csv pipeline-minio:/tmp/credit_bureau.csv

REM Move to correct location
docker exec pipeline-minio sh -c "cp /tmp/credit_bureau.csv /data/banking-data/credit-bureau/"

REM Verify
docker exec pipeline-minio ls -la /data/banking-data/credit-bureau/
```

Or use MinIO Console:
1. Open http://localhost:9003
2. Login with `minioadmin` / `minioadmin`
3. Navigate to `banking-data` bucket
4. Create folder `credit-bureau`
5. Upload `credit_bureau.csv`

### Issue 6: "Job not appearing in Spark UI"

**Error:**
Spark Master UI shows "0 Running, 0 Completed" even though job is running.

**Cause:** SparkSession has `.master("local[*]")` hardcoded, forcing local mode.

**Solution:**
Remove `.master()` from SparkSession builder in your pipeline code:

```java
// File: RealConnectorsPipeline.java or ComprehensiveBankingPipeline.java

SparkSession spark = SparkSession.builder()
    .appName("Real Connectors Pipeline")
    // Remove this line: .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .getOrCreate();
```

Rebuild and resubmit:
```cmd
mvn clean package -DskipTests
run-pipeline-on-cluster.bat RealConnectorsPipeline
```

Now the job will appear in Spark UI with distributed execution across workers.

### Issue 7: "Docker image not found - bitnami/spark:3.5.0"

**Error:**
```
manifest for bitnami/spark:3.5.0 not found: manifest unknown
```

**Solution:**
Already fixed in `docker-compose.yml`:
```yaml
image: bitnami/spark:3.5.3  # Updated from 3.5.0
```

### Issue 8: Build failures with varargs

**Error:**
```
incompatible types: Column[] cannot be converted to Column
```

**Solution:**
Already fixed in all transformation classes. The issue was passing arrays to varargs methods incorrectly. Fixed files:
- `GroupByTransform.java`
- `DropTransform.java`
- `RepartitionTransform.java`
- `UnpivotTransform.java`
- `PivotTransform.java`

If you encounter this, ensure you're using direct array passing:
```java
// ✅ CORRECT
Column[] cols = columns.stream().map(functions::col).toArray(Column[]::new);
return input.select(cols);

// ❌ WRONG
Column first = functions.col(columns.get(0));
Column[] rest = columns.stream().skip(1).map(functions::col).toArray(Column[]::new);
return input.select(first, rest);  // Varargs issue
```

---

## Architecture Overview

### Infrastructure Stack

```
┌─────────────────────────────────────────────────────────────┐
│                    Windows Host System                       │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Docker Desktop (WSL 2)                  │   │
│  │                                                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │   │
│  │  │ Spark Master │  │ Spark Worker │  │ Spark     │ │   │
│  │  │   :7077      │  │   1  :8082   │  │ Worker 2  │ │   │
│  │  │   :8081      │  │              │  │   :8083   │ │   │
│  │  └──────────────┘  └──────────────┘  └───────────┘ │   │
│  │                                                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │   │
│  │  │  PostgreSQL  │  │    MinIO     │  │   Kafka   │ │   │
│  │  │   :5434      │  │  :9002,:9003 │  │   :9092   │ │   │
│  │  └──────────────┘  └──────────────┘  └───────────┘ │   │
│  │                                                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌───────────┐ │   │
│  │  │   pgAdmin    │  │   Kafka UI   │  │ Zookeeper │ │   │
│  │  │   :5050      │  │   :8084      │  │   :2181   │ │   │
│  │  └──────────────┘  └──────────────┘  └───────────┘ │   │
│  │                                                       │   │
│  │              Network: pipeline-network               │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                               │
│  Maven Build → JAR → Mounted to /opt/spark-apps              │
└─────────────────────────────────────────────────────────────┘
```

### Job Execution Flow

```
1. run-pipeline-on-cluster.bat
   │
   ├─→ Check: Spark cluster running
   │
   ├─→ Build: mvn package (if needed)
   │
   └─→ docker exec pipeline-spark-master spark-submit
       │
       ├─→ Load: pipeline-examples-1.0.0-SNAPSHOT.jar
       │
       ├─→ Submit to: spark://spark-master:7077
       │
       └─→ Execution:
           │
           ├─→ Driver (Spark Master container)
           │   └─→ Creates SparkSession
           │       └─→ No .master() → respects spark-submit config
           │
           ├─→ Scheduler (Spark Master)
           │   ├─→ Allocates executors to workers
           │   └─→ Distributes tasks
           │
           └─→ Executors (Workers)
               ├─→ Worker 1: Executors 2, 3
               └─→ Worker 2: Executors 0, 1
                   │
                   ├─→ Read: PostgreSQL (postgres:5432)
                   ├─→ Read: MinIO S3 (minio:9000)
                   ├─→ Write: PostgreSQL (postgres:5432)
                   ├─→ Write: Kafka (kafka:29092)
                   └─→ Write: MinIO S3 (minio:9000)
```

### Data Flow

```
PostgreSQL → Loan Applications
          → Customers
          → Transactions
              ↓
         Spark Cluster
         (2 Workers, 4 Executors)
              ↓
    ┌─────────┼─────────┐
    ↓         ↓         ↓
MinIO S3   Rules     Kafka
Credit   Engine   Messages
Bureau   (Validate,
Data     Transform,
         Filter,
         Business)
    ↓         ↓         ↓
    └─────────┼─────────┘
              ↓
    ┌─────────┴─────────┐
    ↓                   ↓
PostgreSQL          MinIO S3
Results             Results
```

---

## Configuration Reference

### Connection URLs (Docker Network)

```java
// PostgreSQL - Internal Docker network
jdbc:postgresql://postgres:5432/banking

// MinIO S3 - Internal Docker network
http://minio:9000

// Kafka - Internal Docker network
kafka:29092

// External access from Windows:
// PostgreSQL: localhost:5434
// MinIO Console: http://localhost:9003
// MinIO API: http://localhost:9002
// Kafka: localhost:9092
```

### Spark Submit Parameters

```cmd
--master spark://spark-master:7077         # Cluster URL (internal)
--deploy-mode client                       # Driver in submit container
--class com.enterprise.pipeline.examples.RealConnectorsPipeline
--driver-memory 1g                         # Driver heap size
--executor-memory 1g                       # Executor heap size
--executor-cores 1                         # Cores per executor
--num-executors 2                          # Total executors (one per worker)
--conf spark.sql.adaptive.enabled=true     # Adaptive query execution
--conf spark.dynamicAllocation.enabled=false  # Fixed executor count
--packages [dependencies]                  # External JAR dependencies
```

### Environment Credentials

```properties
# PostgreSQL
POSTGRES_USER=pipeline
POSTGRES_PASSWORD=pipeline123
POSTGRES_DB=banking

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# pgAdmin
PGADMIN_EMAIL=admin@admin.com
PGADMIN_PASSWORD=admin
```

---

## Best Practices

### 1. Always Check Services Before Submission

```cmd
docker ps | findstr "pipeline-spark-master"
docker ps | findstr "pipeline-spark-worker"
```

### 2. Monitor Logs During Execution

Open multiple command prompt windows:

```cmd
REM Window 1: Spark Master logs
docker logs -f pipeline-spark-master

REM Window 2: Worker 1 logs
docker logs -f pipeline-spark-worker-1

REM Window 3: Worker 2 logs
docker logs -f pipeline-spark-worker-2
```

### 3. Clean Up Between Runs

```cmd
REM Remove old application data
docker exec pipeline-spark-master rm -rf /tmp/spark-*

REM Clear PostgreSQL results
docker exec -it pipeline-postgres psql -U pipeline -d banking -c "TRUNCATE TABLE loan_approval_results, fraud_detection_results;"
```

### 4. Resource Management

If you need to run other applications, reduce Docker resources:

```yaml
# docker-compose.yml - reduce memory limits
services:
  spark-worker-1:
    environment:
      - SPARK_WORKER_MEMORY=512m  # Reduced from 1g
```

### 5. Development Workflow

```cmd
REM 1. Make code changes
REM 2. Rebuild
mvn clean package -DskipTests

REM 3. Submit job
run-pipeline-on-cluster.bat RealConnectorsPipeline

REM 4. Check results
start http://localhost:8081
```

---

## Stopping and Cleanup

### Stop All Services

```cmd
docker-compose down
```

### Remove All Data (Fresh Start)

```cmd
docker-compose down -v
docker volume prune -f
```

### Remove Everything Including Images

```cmd
docker-compose down -v --rmi all
```

### Start Fresh

```cmd
start-local-environment.bat
```

---

## Additional Resources

### Project Structure

```
java-pipeline/
├── docker-compose.yml                    # Infrastructure definition
├── start-local-environment.bat           # Windows startup script
├── run-pipeline-on-cluster.bat           # Windows job submission script
├── credit_bureau.csv                     # Sample S3 data
├── create_large_dataset.sql              # PostgreSQL test data
├── pipeline-sdk/
│   ├── sdk-core/                         # Core pipeline framework
│   ├── sdk-connectors/                   # JDBC, S3, Kafka connectors
│   ├── sdk-transformations/              # Data transformations
│   └── sdk-rules/                        # Rule engine
└── pipeline-examples/
    ├── pom.xml                            # Maven build (with Shade plugin)
    └── src/main/java/.../examples/
        ├── RealConnectorsPipeline.java   # Full integration example
        └── ComprehensiveBankingPipeline.java
```

### Key Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Defines all services, networks, volumes |
| `start-local-environment.bat` | Starts infrastructure with health checks |
| `run-pipeline-on-cluster.bat` | Submits job to Spark cluster |
| `pipeline-examples/pom.xml` | Maven build with Shade plugin for uber JAR |
| `RealConnectorsPipeline.java` | Example pipeline with all connectors |

### Critical Configuration Points

1. **SparkSession:** No `.master()` in code (line 55-66 in RealConnectorsPipeline.java)
2. **Docker URLs:** Use service names not localhost (lines 41-50 in RealConnectorsPipeline.java)
3. **Maven Shade:** Exclude signature files (pipeline-examples/pom.xml)
4. **Spark Image:** Use bitnami/spark:3.5.3 (docker-compose.yml)
5. **Port Mappings:** Adjusted to avoid conflicts (docker-compose.yml)

---

## Success Checklist

Before considering deployment successful, verify:

- [ ] Docker containers: 9 services running
- [ ] Spark Master UI: http://localhost:8081 shows 2 workers
- [ ] Build: `mvn package` succeeds without errors
- [ ] Job submission: Script executes without errors
- [ ] Spark UI: Shows 1 completed application after run
- [ ] Executors: 4 executors distributed across 2 workers
- [ ] PostgreSQL: Results tables populated with data
- [ ] MinIO: Output files visible in pipeline-output bucket
- [ ] Kafka: Messages visible in loan-approval-results topic

---

## Contact and Support

For issues or questions:

1. Check Spark Master UI: http://localhost:8081
2. Check Worker logs: `docker logs pipeline-spark-worker-1`
3. Verify services: `docker ps`
4. Review this guide's troubleshooting section

---

**Document Version:** 1.0
**Last Updated:** 2025-11-07
**Tested On:** Windows 10/11, Docker Desktop 4.x, Spark 3.5.3
**Status:** Production Ready ✅
