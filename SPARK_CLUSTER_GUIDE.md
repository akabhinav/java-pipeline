# Spark Cluster Testing Guide

Complete guide for running pipelines on the **local Spark cluster** using Docker.

---

## 🎯 What's Different: Local vs Cluster Mode

| Mode | Spark Master | Resources | Use Case |
|------|--------------|-----------|----------|
| **Local** (`local[*]`) | Single JVM | Your machine only | Development, debugging |
| **Cluster** (`spark://localhost:7077`) | 1 Master + 2 Workers | Distributed across containers | Production-like testing |

---

## 🏗️ Spark Cluster Architecture

When you run `start-local-environment.bat`, you get a **real Spark cluster**:

```
┌─────────────────────────────────────────────────────────────┐
│                     Spark Master                             │
│                 spark://spark-master:7077                    │
│                  http://localhost:8081                       │
│   • Manages cluster resources                               │
│   • Schedules jobs across workers                           │
│   • Monitors worker health                                  │
└─────────────────────────────────────────────────────────────┘
              │                        │
              ▼                        ▼
    ┌──────────────────┐      ┌──────────────────┐
    │  Worker 1        │      │  Worker 2        │
    │  localhost:8082  │      │  localhost:8083  │
    │  • 2 GB RAM      │      │  • 2 GB RAM      │
    │  • 2 CPU cores   │      │  • 2 CPU cores   │
    └──────────────────┘      └──────────────────┘
```

**Total Cluster Resources:**
- 4 CPU cores
- 4 GB memory
- 2 executor instances

---

## 🚀 Quick Start

### 1. Start the Cluster

```bash
# Windows
start-local-environment.bat

# Mac/Linux
./start-local-environment.sh
```

**Verify Cluster is Running:**
Open http://localhost:8081

You should see:
- ✅ **Workers:** 2 ALIVE
- ✅ **Cores:** 4 Total
- ✅ **Memory:** 4.0 GB Total

### 2. Run Pipeline on Cluster

```bash
# Windows
run-pipeline-on-cluster.bat RealConnectorsPipeline

# Mac/Linux
./run-pipeline-on-cluster.sh RealConnectorsPipeline
```

**What This Does:**
1. Checks if cluster is running
2. Builds the project (if needed)
3. Submits job to Spark Master
4. Distributes work across 2 workers
5. Shows results

---

## 📊 Monitoring Your Jobs

### Spark Master UI
**URL:** http://localhost:8081

**What You See:**
- **Running Applications**: Currently executing jobs
- **Completed Applications**: Finished jobs
- **Workers**: Status of all worker nodes
- **Resource Usage**: CPU, memory per worker

### Worker UIs
**URLs:**
- Worker 1: http://localhost:8082
- Worker 2: http://localhost:8083

**What You See:**
- Executor details
- Tasks being processed
- Logs for this specific worker

### History Server
**URL:** http://localhost:18080

**What You See:**
- All completed jobs (even after they finish)
- Detailed execution metrics
- Stage and task timelines
- SQL query plans

---

## 🎯 Running Different Pipelines

### Option 1: Real Connectors Pipeline (Default)

```bash
# Windows
run-pipeline-on-cluster.bat RealConnectorsPipeline

# Mac/Linux
./run-pipeline-on-cluster.sh RealConnectorsPipeline
```

**What It Tests:**
- PostgreSQL → Load data
- MinIO (S3) → Load credit data
- Rule Engine → Apply all 4 rule types
- PostgreSQL → Save results
- MinIO (S3) → Upload results
- Kafka → Publish messages

### Option 2: Comprehensive Banking Pipeline

```bash
run-pipeline-on-cluster.bat ComprehensiveBankingPipeline
# or
./run-pipeline-on-cluster.sh ComprehensiveBankingPipeline
```

**What It Tests:**
- All 5 banking scenarios
- In-memory data processing
- No external connectors needed

### Option 3: Custom Pipeline

```bash
# Your own pipeline class
run-pipeline-on-cluster.bat MyCustomPipeline
```

---

## 📈 Performance Comparison

### Local Mode (Single JVM)

```java
SparkSession spark = SparkSession.builder()
    .master("local[*]")  // Uses all cores on your machine
    .getOrCreate();
```

**Pros:**
- ✅ Faster startup
- ✅ Easier debugging
- ✅ No network overhead

**Cons:**
- ❌ Not production-like
- ❌ Limited scalability
- ❌ No fault tolerance

### Cluster Mode (Distributed)

```java
SparkSession spark = SparkSession.builder()
    .master("spark://spark-master:7077")
    .getOrCreate();
```

**Pros:**
- ✅ Production-like environment
- ✅ Tests distributed execution
- ✅ Worker fault tolerance
- ✅ Resource management

**Cons:**
- ❌ Slower startup (network, scheduling)
- ❌ More complex debugging

---

## 🔍 Understanding Cluster Execution

### What Happens When You Submit a Job

1. **Submission** → Job sent to Spark Master (port 7077)
2. **Resource Allocation** → Master assigns executors on workers
3. **Task Distribution** → Work split across 2 workers
4. **Execution** → Each worker processes its partition
5. **Results Collection** → Master collects results
6. **Completion** → Job finishes, resources released

### Example: Processing 15 Loan Applications

```
Master receives job:
┌────────────────────────────────────────┐
│ 15 loan applications to process        │
└────────────────────────────────────────┘
              │
              ▼
    Master splits work:
    ┌──────────────┐        ┌──────────────┐
    │  Worker 1    │        │  Worker 2    │
    │  Apps 1-7    │        │  Apps 8-15   │
    │  (7 records) │        │  (8 records) │
    └──────────────┘        └──────────────┘
              │                      │
              └──────────┬───────────┘
                         ▼
              Master collects results:
              ┌─────────────────────┐
              │ 15 processed results │
              └─────────────────────┘
```

---

## 🛠️ Advanced Configuration

### Adjust Worker Resources

Edit `docker-compose.yml`:

```yaml
spark-worker-1:
  environment:
    - SPARK_WORKER_MEMORY=4G    # Change from 2G to 4G
    - SPARK_WORKER_CORES=4      # Change from 2 to 4
```

Then restart:
```bash
docker-compose restart spark-worker-1
```

### Add More Workers

Add to `docker-compose.yml`:

```yaml
spark-worker-3:
  image: bitnami/spark:3.5.0
  container_name: pipeline-spark-worker-3
  depends_on:
    spark-master:
      condition: service_healthy
  ports:
    - "8084:8081"
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    - SPARK_WORKER_MEMORY=2G
    - SPARK_WORKER_CORES=2
```

### Scale Workers Dynamically

```bash
# Scale to 4 workers
docker-compose up -d --scale spark-worker-1=4

# Note: You need to configure port mapping for additional workers
```

---

## 🐛 Troubleshooting

### Issue 1: Workers Not Connecting to Master

**Symptoms:**
- Master UI shows 0 workers
- Jobs stay in "SUBMITTED" state

**Solution:**
```bash
# Check worker logs
docker logs pipeline-spark-worker-1

# Restart workers
docker-compose restart spark-worker-1 spark-worker-2

# Verify network
docker exec pipeline-spark-worker-1 ping spark-master
```

### Issue 2: Out of Memory Errors

**Error:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solution 1: Increase Worker Memory**
```bash
# Edit docker-compose.yml
SPARK_WORKER_MEMORY=4G  # Was 2G
```

**Solution 2: Reduce Parallelism**
```bash
# In your pipeline code
spark.conf().set("spark.default.parallelism", "2")
spark.conf().set("spark.sql.shuffle.partitions", "4")
```

### Issue 3: Job Takes Too Long

**Symptoms:**
- Job running for > 5 minutes on small dataset

**Solution:**
```bash
# Check if data is skewed
# View Spark UI → Stages → Check task distribution

# Check if workers are idle
# View Worker UIs → Should show active executors

# Check logs
docker logs pipeline-spark-master
```

### Issue 4: Cannot Submit Jobs

**Error:**
```
Connection refused: spark://localhost:7077
```

**Solution:**
```bash
# Check if master is running
docker ps | grep spark-master

# Check master logs
docker logs pipeline-spark-master

# Restart cluster
docker-compose restart spark-master spark-worker-1 spark-worker-2
```

---

## 📊 Performance Tips

### 1. Optimize Partitioning

```java
// Repartition before expensive operations
dataset.repartition(4)  // Match number of cores
       .map(expensiveTransformation)
```

### 2. Cache Intermediate Results

```java
// Cache data that's used multiple times
Dataset<Row> enriched = loans.join(customers, "id");
enriched.cache();  // Keep in memory

// Use it multiple times
enriched.filter("amount > 1000").count();
enriched.groupBy("status").count();
```

### 3. Broadcast Small Tables

```java
// Broadcast small lookup tables
import static org.apache.spark.sql.functions.broadcast;

loans.join(broadcast(smallReferenceTable), "id")
```

### 4. Enable Adaptive Query Execution

Already configured in `docker/spark/conf/spark-defaults.conf`:
```properties
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

---

## 📚 Comparing Modes

### When to Use Local Mode

✅ **Use Local Mode When:**
- Developing new transformations
- Debugging issues
- Running unit tests
- Working with small datasets (< 1 GB)
- Need fast iteration

**Run:**
```bash
cd pipeline-examples
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.RealConnectorsPipeline"
```

### When to Use Cluster Mode

✅ **Use Cluster Mode When:**
- Testing production deployment
- Working with large datasets (> 1 GB)
- Testing distributed execution
- Validating resource requirements
- Performance testing

**Run:**
```bash
./run-pipeline-on-cluster.sh RealConnectorsPipeline
```

---

## 🎓 Learning Exercises

### Exercise 1: Compare Execution Times

```bash
# Run in local mode
time mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.RealConnectorsPipeline"

# Run in cluster mode
time ./run-pipeline-on-cluster.sh RealConnectorsPipeline

# Compare: Which is faster? Why?
```

### Exercise 2: Watch Task Distribution

1. Open Spark Master UI: http://localhost:8081
2. Run pipeline: `./run-pipeline-on-cluster.sh RealConnectorsPipeline`
3. Click on running application
4. Go to "Stages" tab
5. Observe tasks distributed across workers

### Exercise 3: Simulate Worker Failure

```bash
# Start pipeline
./run-pipeline-on-cluster.sh RealConnectorsPipeline &

# Kill a worker mid-execution
docker stop pipeline-spark-worker-1

# Watch Spark reschedule tasks to Worker 2
```

---

## 🔐 Production Deployment

### Differences from Local Cluster

| Aspect | Local Cluster | Production |
|--------|--------------|------------|
| **Master** | 1 container | 1-3 nodes (HA) |
| **Workers** | 2 containers | 10-100+ nodes |
| **Memory** | 2 GB per worker | 32-128 GB per worker |
| **Cores** | 2 per worker | 8-32 per worker |
| **Network** | Docker bridge | 10 Gbps datacenter |
| **Storage** | Docker volumes | HDFS/S3 |
| **Security** | None | Kerberos, TLS |
| **Monitoring** | Basic UI | Prometheus, Grafana |

### Deployment Options

**Option 1: Standalone Cluster**
```bash
# Start master
$SPARK_HOME/sbin/start-master.sh

# Start workers (on each node)
$SPARK_HOME/sbin/start-worker.sh spark://master-host:7077
```

**Option 2: YARN Cluster**
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 50 \
  --executor-memory 16g \
  --executor-cores 4 \
  ...
```

**Option 3: Kubernetes**
```bash
spark-submit \
  --master k8s://https://kubernetes-api:6443 \
  --deploy-mode cluster \
  --conf spark.executor.instances=50 \
  ...
```

---

## 📊 Cluster Health Checks

### Quick Status Check

```bash
# Check all Spark services
docker ps | grep spark

# Should show 4 containers:
# - pipeline-spark-master
# - pipeline-spark-worker-1
# - pipeline-spark-worker-2
# - pipeline-spark-history
```

### Detailed Status

```bash
# Master status
curl http://localhost:8081/json/ | jq '.workers'

# Worker 1 status
curl http://localhost:8082/json/ | jq '.cores'

# Worker 2 status
curl http://localhost:8083/json/ | jq '.memory'
```

### View Recent Jobs

```bash
# List recent applications
curl http://localhost:8081/api/v1/applications

# Get specific application details
curl http://localhost:8081/api/v1/applications/app-XXXXXX
```

---

## 🚀 Next Steps

1. **Start Cluster**: `start-local-environment.bat`
2. **Verify UIs**: Open http://localhost:8081
3. **Run First Job**: `run-pipeline-on-cluster.bat RealConnectorsPipeline`
4. **Monitor Execution**: Watch Spark Master UI
5. **Check Results**: View PostgreSQL, MinIO, Kafka
6. **Experiment**: Adjust worker resources, add data, modify rules

---

## 📚 Additional Resources

- **Spark Documentation**: https://spark.apache.org/docs/3.5.0/
- **Bitnami Spark Images**: https://github.com/bitnami/containers/tree/main/bitnami/spark
- **Spark Cluster Mode**: https://spark.apache.org/docs/latest/cluster-overview.html
- **Spark Monitoring**: https://spark.apache.org/docs/latest/monitoring.html

---

**Happy Cluster Computing! ⚡**
