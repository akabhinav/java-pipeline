# Local Testing Environment Guide

Complete guide for running the Enterprise Data Pipeline Platform with **real infrastructure** on your local machine.

---

## 🎯 What You Get

This setup provides a **complete production-like environment** running locally:

| Service | Purpose | URL | Credentials |
|---------|---------|-----|-------------|
| **Kafka** | Streaming platform | localhost:9092 | - |
| **Kafka UI** | Kafka management | http://localhost:8080 | - |
| **MinIO (S3)** | Object storage | http://localhost:9001 | minioadmin/minioadmin |
| **PostgreSQL** | Relational database | localhost:5432 | pipeline/pipeline123 |
| **pgAdmin** | Database UI | http://localhost:5050 | admin@pipeline.com/admin123 |

**Sample Data Included:**
- ✅ 15 customers
- ✅ 15 loan applications
- ✅ 30 transactions
- ✅ 15 credit bureau records

---

## 📋 Prerequisites

### 1. Install Docker Desktop

**Windows:**
1. Download from: https://www.docker.com/products/docker-desktop/
2. Install and restart your computer
3. Open Docker Desktop and ensure it's running
4. Verify: Open PowerShell and run `docker --version`

**Mac:**
1. Download from: https://www.docker.com/products/docker-desktop/
2. Install and start Docker Desktop
3. Verify: Open Terminal and run `docker --version`

**Linux:**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose

# Verify
docker --version
docker-compose --version
```

### 2. Install Java 17+

**Windows:**
1. Download from: https://adoptium.net/
2. Install and set JAVA_HOME environment variable

**Mac:**
```bash
brew install openjdk@17
```

**Linux:**
```bash
sudo apt-get install openjdk-17-jdk
```

### 3. Install Maven

**Windows:**
1. Download from: https://maven.apache.org/download.cgi
2. Extract and add to PATH

**Mac:**
```bash
brew install maven
```

**Linux:**
```bash
sudo apt-get install maven
```

---

## 🚀 Quick Start (One Command)

### Windows (PowerShell or CMD):
```cmd
start-local-environment.bat
```

### Mac/Linux:
```bash
./start-local-environment.sh
```

This single command will:
1. ✅ Start all Docker containers (Kafka, MinIO, PostgreSQL)
2. ✅ Wait for services to be healthy
3. ✅ Upload sample data to MinIO (S3)
4. ✅ Load sample data into PostgreSQL
5. ✅ Create Kafka topics
6. ✅ Display service URLs

**Expected Output:**
```
================================================================================
  Environment Ready!
================================================================================

Service URLs:
  Kafka UI:       http://localhost:8080
  MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)
  pgAdmin:        http://localhost:5050  (admin@pipeline.com/admin123)
  PostgreSQL:     localhost:5432         (pipeline/pipeline123)

Sample Data Loaded:
  ✓ PostgreSQL: 15 customers, 15 loan applications, 30 transactions
  ✓ MinIO (S3): customers.csv, credit_bureau.csv
  ✓ Kafka: 4 topics created

Next Steps:
  1. View services:  docker-compose ps
  2. Run pipeline:   See instructions below
  3. Stop services:  start-local-environment.bat --stop
```

---

## 🔧 Step-by-Step Setup (Manual)

### Step 1: Start Services

```bash
# Windows
start-local-environment.bat

# Mac/Linux
./start-local-environment.sh
```

### Step 2: Verify Services are Running

```bash
docker-compose ps
```

You should see all services "Up" and "healthy":
```
NAME                   STATUS              PORTS
pipeline-kafka         Up (healthy)        9092, 9093
pipeline-kafka-ui      Up                  8080
pipeline-minio         Up (healthy)        9000, 9001
pipeline-postgres      Up (healthy)        5432
pipeline-pgadmin       Up                  5050
pipeline-zookeeper     Up (healthy)        2181
```

### Step 3: Build the Project

```bash
cd /path/to/java-pipeline
mvn clean install -DskipTests
```

### Step 4: Run the Real Connectors Pipeline

```bash
cd pipeline-examples

# Option 1: Maven exec
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.RealConnectorsPipeline"

# Option 2: Spark submit (if you have Spark installed)
spark-submit \
  --class com.enterprise.pipeline.examples.RealConnectorsPipeline \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
  target/pipeline-examples-1.0-SNAPSHOT.jar
```

---

## 📊 What the Pipeline Does

The `RealConnectorsPipeline` demonstrates **complete integration**:

### 1. **Load from PostgreSQL** (JDBC)
```
✓ 15 loan applications
✓ 15 customers
✓ 30 transactions
```

### 2. **Load from MinIO (S3)**
```
✓ Credit bureau data (CSV)
```

### 3. **Apply Rule Engine**
```
✓ Validation rules (data quality)
✓ Transformation rules (credit score grading, EMI)
✓ Business rules (loan approval, fraud detection)
✓ Filter rules (risk assessment)
```

### 4. **Save Results**
```
✓ PostgreSQL → loan_approval_results table
✓ MinIO (S3) → pipeline-output/loan-approval-results/
✓ Kafka → loan-approval-results topic
```

### 5. **Fraud Detection**
```
✓ Analyze transactions
✓ Save to fraud_detection_results table
```

---

## 🔍 Verify Results

### 1. PostgreSQL (via psql)

**Windows (PowerShell):**
```powershell
docker exec -it pipeline-postgres psql -U pipeline -d banking
```

**Mac/Linux:**
```bash
psql -h localhost -U pipeline -d banking
# Password: pipeline123
```

**Queries:**
```sql
-- View loan approval results
SELECT * FROM loan_approval_results;

-- View fraud detection results
SELECT * FROM fraud_detection_results;

-- Statistics
SELECT loan_status, COUNT(*) FROM loan_approval_results GROUP BY loan_status;
```

### 2. PostgreSQL (via pgAdmin Web UI)

1. Open: http://localhost:5050
2. Login: `admin@pipeline.com` / `admin123`
3. Servers → Pipeline PostgreSQL (auto-configured)
4. Navigate: Servers → Pipeline PostgreSQL → Databases → banking → Schemas → public → Tables

### 3. MinIO (S3)

1. Open: http://localhost:9001
2. Login: `minioadmin` / `minioadmin`
3. Navigate to:
   - **banking-data** bucket → Input data (customers.csv, credit_bureau.csv)
   - **pipeline-output** bucket → Results (loan-approval-results/)

### 4. Kafka

**Option A: Kafka UI (Web Interface)**
1. Open: http://localhost:8080
2. Navigate to Topics
3. View: `loan-approval-results`, `fraud-detection-results`
4. Browse messages

**Option B: Command Line**
```bash
# List topics
docker exec pipeline-kafka kafka-topics --list --bootstrap-server localhost:9092

# Consume messages
docker exec pipeline-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic loan-approval-results \
  --from-beginning \
  --max-messages 10
```

---

## 🛠️ Useful Commands

### Start/Stop Environment

```bash
# Windows
start-local-environment.bat              # Start all services
start-local-environment.bat --stop       # Stop all services
start-local-environment.bat --restart    # Restart
start-local-environment.bat --status     # Check status
start-local-environment.bat --logs       # View logs

# Mac/Linux
./start-local-environment.sh             # Start all services
./start-local-environment.sh --stop      # Stop all services
./start-local-environment.sh --restart   # Restart
./start-local-environment.sh --status    # Check status
./start-local-environment.sh --logs      # View logs
```

### Docker Compose Commands

```bash
docker-compose up -d              # Start in background
docker-compose down               # Stop and remove containers
docker-compose ps                 # Check status
docker-compose logs -f            # View logs (all services)
docker-compose logs -f kafka      # View Kafka logs only
docker-compose restart kafka      # Restart specific service
```

### Access Individual Services

```bash
# PostgreSQL
docker exec -it pipeline-postgres psql -U pipeline -d banking

# Kafka Shell
docker exec -it pipeline-kafka bash

# MinIO Shell
docker exec -it pipeline-minio sh

# View container logs
docker logs pipeline-kafka -f
docker logs pipeline-postgres -f
```

---

## 🐛 Troubleshooting

### Issue 1: Port Already in Use

**Error:**
```
Error: bind: address already in use 0.0.0.0:5432
```

**Solution:**
```bash
# Check what's using the port (Windows PowerShell)
netstat -ano | findstr :5432

# Check what's using the port (Mac/Linux)
lsof -i :5432

# Either stop the conflicting service or change the port in docker-compose.yml
```

### Issue 2: Docker Not Running

**Error:**
```
Cannot connect to the Docker daemon
```

**Solution:**
- **Windows:** Start Docker Desktop application
- **Mac:** Start Docker Desktop application
- **Linux:** `sudo systemctl start docker`

### Issue 3: Services Not Healthy

**Error:**
```
pipeline-kafka is unhealthy
```

**Solution:**
```bash
# Check logs
docker logs pipeline-kafka

# Restart specific service
docker-compose restart kafka

# Full restart
docker-compose down
docker-compose up -d
```

### Issue 4: Maven Build Fails

**Error:**
```
Could not resolve dependencies
```

**Solution:**
```bash
# Clear Maven cache
mvn clean

# Force update
mvn clean install -U -DskipTests

# Check internet connection (Maven needs to download dependencies)
```

### Issue 5: Out of Memory

**Error:**
```
java.lang.OutOfMemoryError
```

**Solution:**

**For Docker Desktop:**
- Open Docker Desktop Settings
- Resources → Memory → Increase to 8GB
- Apply & Restart

**For Spark:**
```bash
# Increase driver memory
spark-submit --driver-memory 4g --executor-memory 4g ...
```

### Issue 6: Cannot Connect to PostgreSQL from Host

**Error:**
```
Connection refused: localhost:5432
```

**Solution:**
```bash
# Check if PostgreSQL container is running
docker ps | grep postgres

# Check if port is exposed
docker port pipeline-postgres

# Try connecting with Docker network
docker exec -it pipeline-postgres psql -U pipeline -d banking
```

---

## 📊 Sample Data Overview

### PostgreSQL Tables

| Table | Records | Description |
|-------|---------|-------------|
| `customers` | 15 | Customer master data |
| `loan_applications` | 15 | Loan application requests |
| `credit_bureau_data` | 15 | Credit scores and history |
| `transactions` | 30 | Customer transactions |

### MinIO (S3) Files

| Bucket | File | Size | Description |
|--------|------|------|-------------|
| `banking-data` | customers.csv | ~2KB | Customer data |
| `banking-data` | credit_bureau.csv | ~1KB | Credit bureau data |

### Kafka Topics

| Topic | Partitions | Description |
|-------|------------|-------------|
| `loan-applications` | 3 | Incoming loan applications |
| `transactions` | 3 | Transaction stream |
| `loan-approval-results` | 3 | Pipeline output |
| `fraud-detection-results` | 3 | Fraud analysis results |

---

## 🎓 Learning Path

### 1. **Start with Sample Pipeline**
```bash
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.ComprehensiveBankingPipeline"
```
This uses in-memory data (no external services needed).

### 2. **Run Real Connectors Pipeline**
```bash
# First start environment
start-local-environment.bat  # or .sh on Mac/Linux

# Then run pipeline
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.RealConnectorsPipeline"
```
This connects to real PostgreSQL, MinIO, and Kafka.

### 3. **Modify and Experiment**
- Add custom rules
- Change data sources
- Add new transformations
- Publish to different Kafka topics

---

## 🔐 Production Considerations

### Security

**Local (Development):**
- ✅ Simple passwords (minioadmin, pipeline123)
- ✅ No SSL/TLS
- ✅ Public bucket policies

**Production:**
- ❌ Use strong passwords or IAM roles
- ❌ Enable SSL/TLS for all services
- ❌ Implement proper access controls
- ❌ Use secrets management (Vault, AWS Secrets Manager)

### Scalability

**Local:**
- Single node
- Limited to your machine resources

**Production:**
- Kafka cluster (3+ brokers)
- PostgreSQL with replication
- S3 (real AWS S3, not MinIO)
- Spark cluster (YARN/Kubernetes)

### Configuration

For production, update connection strings in the pipeline code:

```java
// Local
private static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/banking";
private static final String S3_ENDPOINT = "http://localhost:9000";

// Production
private static final String POSTGRES_URL = "jdbc:postgresql://prod-db.example.com:5432/banking";
private static final String S3_ENDPOINT = "https://s3.amazonaws.com";  // Real S3
```

---

## 📚 Additional Resources

- **Main README**: [README.md](README.md)
- **Architecture**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Rule Engine**: [RULE_ENGINE.md](RULE_ENGINE.md)
- **Test Output**: [PIPELINE_TEST_OUTPUT.md](PIPELINE_TEST_OUTPUT.md)

---

## 💡 Next Steps

1. ✅ **Run the environment**: `start-local-environment.bat`
2. ✅ **Verify services**: Open http://localhost:8080 (Kafka UI)
3. ✅ **Build project**: `mvn clean install -DskipTests`
4. ✅ **Run pipeline**: See "Run the Real Connectors Pipeline" section
5. ✅ **Verify results**: Check PostgreSQL, MinIO, Kafka
6. ✅ **Experiment**: Modify rules, add transformations
7. ✅ **Deploy**: Move to production cluster

---

## 🤝 Support

For issues:
1. Check troubleshooting section above
2. View logs: `docker-compose logs -f`
3. Check service status: `docker-compose ps`
4. Review GitHub issues

---

**Happy Testing! 🚀**
