# SDK Orchestrator - Workflow Management

**DAG-based workflow orchestration for data pipelines** - similar to Airflow, Prefect, and Dagster.

---

## 🎯 What is SDK Orchestrator?

A **lightweight, Java-based workflow orchestration engine** for managing multi-job data pipelines.

### Key Difference from Spark DAG

| Spark's Internal DAG | SDK Orchestrator DAG |
|---------------------|---------------------|
| Optimizes transformations **within** a single job | Manages **multiple** independent jobs/workflows |
| `filter → join → groupBy` | Ingest Job → Transform Job → Load Job |
| Internal optimization | External workflow management |
| Automatic | You define it |

**Think:** Spark DAG = car engine (internal), Orchestrator DAG = traffic system (external)

---

## ⚡ Quick Start

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

---

## 🏗️ Core Features

### 1. **DAG-Based Workflows**
Define job dependencies:
```java
DAG dag = DAG.builder("workflow")
    .addJob(jobA)
    .addJob(jobB).addDependency("jobB", "jobA")  // B depends on A
    .addJob(jobC).addDependency("jobC", "jobA")  // C depends on A
    .addJob(jobD).addDependencies("jobD", List.of("jobB", "jobC"))  // D depends on B and C
    .build();
```

**Execution:**
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

### 2. **Parallel Execution**
Jobs at the same dependency level run in parallel:
```java
DAGExecutor executor = new DAGExecutor(4);  // 4 parallel threads
DAGExecutionResult result = executor.execute(dag, parameters);
```

### 3. **Retry Policies**
Automatic retry on failure:
```java
// Exponential backoff: 2s, 4s, 8s, 16s, ...
RetryPolicy.exponentialBackoff(5)  // 5 attempts

// Fixed delay: 10s between each retry
RetryPolicy.fixedDelay(3, Duration.ofSeconds(10))

// Linear backoff: 5s, 10s, 15s, ...
RetryPolicy.linearBackoff(4, Duration.ofSeconds(5), Duration.ofMinutes(1))
```

### 4. **SLA Monitoring**
Track execution time and alert on violations:
```java
SLAMonitor monitor = SLAMonitor.builder()
    .threshold(Duration.ofHours(1))
    .onViolation(violation -> sendSlackAlert(violation))
    .build();
```

### 5. **Cron Scheduling**
Schedule workflows to run automatically:
```java
Pipeline.create("daily-reports")
    .addJob("generate", reportJob)
    .schedule("0 0 * * *")  // Daily at midnight
    .startScheduled();
```

**Cron Examples:**
- `"0 0 * * *"` - Daily at midnight
- `"0 */6 * * *"` - Every 6 hours
- `"0 0 * * MON"` - Every Monday
- `"0 0 1 * *"` - First day of month

---

## 📖 Complete Example

```java
import com.enterprise.pipeline.orchestrator.*;
import com.enterprise.pipeline.orchestrator.core.*;

public class ETLPipeline {
    public static void main(String[] args) {
        // Define jobs
        Job ingest = new IngestJob();
        Job clean = new CleanJob();
        Job aggregate = new AggregateJob();
        Job load = new LoadJob();
        Job notify = new NotifyJob();

        // Build pipeline
        DAGExecutionResult result = Pipeline.create("daily-etl")
            // Level 0: Ingest
            .addJob("ingest", ingest)

            // Level 1: Clean (depends on ingest)
            .addJob("clean", clean)
                .dependsOn("ingest")

            // Level 2: Aggregate (depends on clean)
            .addJob("aggregate", aggregate)
                .dependsOn("clean")

            // Level 3: Load and Notify (both depend on aggregate)
            .addJob("load", load)
                .dependsOn("aggregate")
            .addJob("notify", notify)
                .dependsOn("aggregate")

            // Configuration
            .withParameters(Map.of(
                "date", LocalDate.now(),
                "environment", "production"
            ))
            .retry(RetryPolicy.exponentialBackoff(3))
            .sla(Duration.ofHours(2))
            .maxParallelism(8)
            .run();

        // Check results
        if (result.isSuccess()) {
            System.out.println("Pipeline succeeded!");
        } else {
            System.err.println("Pipeline failed!");
            result.getJobResults().forEach((jobId, jobResult) -> {
                if (jobResult.isFailure()) {
                    System.err.println("Failed job: " + jobId);
                    jobResult.getError().ifPresent(Throwable::printStackTrace);
                }
            });
        }
    }
}
```

---

## 🔄 Job Implementation

```java
import com.enterprise.pipeline.orchestrator.core.*;

public class MyJob implements Job {

    @Override
    public String getId() {
        return "my-job";
    }

    @Override
    public String getName() {
        return "My Custom Job";
    }

    @Override
    public JobResult execute(JobContext context) throws Exception {
        Instant start = Instant.now();

        // Get parameters
        String date = context.getParameter("date")
            .orElse(LocalDate.now().toString()).toString();

        // Get output from previous job
        context.getPreviousResult("previous-job-id")
            .ifPresent(result -> {
                Map<String, Object> output = result.getOutput();
                // Use previous job output
            });

        // Do work
        System.out.println("Processing data for: " + date);
        processData(date);

        // Share state with downstream jobs
        context.setSharedState("recordCount", 1000);

        return JobResult.builder(getId())
            .status(JobStatus.SUCCESS)
            .startTime(start)
            .endTime(Instant.now())
            .output(Map.of(
                "recordsProcessed", 1000,
                "outputPath", "s3://bucket/output"
            ))
            .build();
    }

    @Override
    public Duration getTimeout() {
        return Duration.ofMinutes(30);  // Job-specific timeout
    }

    @Override
    public boolean isRetryable() {
        return true;  // Allow retries on failure
    }
}
```

---

## 📅 Scheduled Execution

```java
import com.enterprise.pipeline.orchestrator.scheduler.CronScheduler;

// Create pipeline
Pipeline pipeline = Pipeline.create("hourly-sync")
    .addJob("sync", syncJob)
    .schedule("0 * * * *");  // Every hour

// Start scheduler
CronScheduler scheduler = pipeline.startScheduled();

// Keep running...
// scheduler.stop() when done
```

---

## 🎛️ Advanced Features

### Conditional Execution
```java
public class ConditionalJob implements Job {
    @Override
    public JobResult execute(JobContext context) {
        if (shouldSkip(context)) {
            return JobResult.builder(getId())
                .status(JobStatus.SKIPPED)
                .startTime(Instant.now())
                .endTime(Instant.now())
                .build();
        }
        // Execute normally
    }
}
```

### Custom Retry Logic
```java
DAGExecutor executor = new DAGExecutor(
    4,  // parallelism
    RetryPolicy.exponentialBackoff(5, Duration.ofSeconds(1), Duration.ofMinutes(10)),
    SLAMonitor.builder()
        .threshold(Duration.ofHours(1))
        .onViolation(v -> sendPagerDutyAlert(v))
        .build()
);
```

### Access Job Results
```java
DAGExecutionResult result = pipeline.run();

// Overall status
System.out.println("Status: " + result.getOverallStatus());
System.out.println("Duration: " + result.getDuration());

// Individual job results
result.getJobResults().forEach((jobId, jobResult) -> {
    System.out.println(jobId + ": " + jobResult.getStatus());
    System.out.println("  Duration: " + jobResult.getDuration());
    System.out.println("  Output: " + jobResult.getOutput());

    if (jobResult.isFailure()) {
        jobResult.getError().ifPresent(e -> e.printStackTrace());
    }
});
```

---

## 🆚 SDK Orchestrator vs Airflow

| Feature | SDK Orchestrator | Airflow |
|---------|-----------------|---------|
| **Language** | Java | Python |
| **Deployment** | Embedded (single JAR) | Separate cluster |
| **Infrastructure** | None (in-memory) | PostgreSQL, web server, scheduler |
| **Type Safety** | Compile-time | Runtime |
| **UI** | None (logs only) | Rich web UI |
| **Maturity** | New | Battle-tested |
| **Best For** | Embedded workflows, Java apps | Enterprise workflows, large teams |

---

## ✅ When to Use SDK Orchestrator

✅ Building a **platform/SDK** with embedded orchestration
✅ **Java-only** environment (no Python allowed)
✅ **Simple workflows** (Airflow is overkill)
✅ **Lightweight deployments** (no infrastructure overhead)
✅ **Type-safe** compile-time validation needed

---

## ❌ When to Use Airflow Instead

❌ Need **rich web UI** and monitoring
❌ **Complex workflows** (100+ jobs)
❌ **Large teams** need collaboration features
❌ Already using **Python** ecosystem
❌ Need **enterprise support**

---

## 📊 Architecture

```
┌─────────────────────────────────────────┐
│           Pipeline API                  │
│  (High-level fluent interface)          │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│            DAG Builder                  │
│  (Builds directed acyclic graph)        │
│  - Cycle detection                      │
│  - Topological sorting                  │
│  - Parallel execution planning          │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│           DAG Executor                  │
│  (Executes jobs level-by-level)         │
│  - Thread pool management               │
│  - Dependency checking                  │
│  - Retry handling                       │
│  - Timeout management                   │
└─────────┬───────────────┬───────────────┘
          │               │
┌─────────▼─────┐  ┌──────▼──────┐
│  Retry Policy │  │ SLA Monitor │
│  - Fixed      │  │ - Threshold │
│  - Linear     │  │ - Alerts    │
│  - Exponential│  │             │
└───────────────┘  └─────────────┘
```

---

## 🚀 Running the Example

```bash
# Build
cd sdk-orchestrator
mvn clean install

# Run example
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.orchestrator.examples.SimpleETLPipeline"
```

---

## 📚 Additional Resources

- [Job Interface](src/main/java/com/enterprise/pipeline/orchestrator/core/Job.java)
- [DAG Engine](src/main/java/com/enterprise/pipeline/orchestrator/dag/DAG.java)
- [Pipeline API](src/main/java/com/enterprise/pipeline/orchestrator/Pipeline.java)
- [Example](src/main/java/com/enterprise/pipeline/orchestrator/examples/SimpleETLPipeline.java)

---

**SDK Orchestrator - Lightweight workflow orchestration for Java data pipelines**
