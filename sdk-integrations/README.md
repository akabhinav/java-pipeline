# SDK Integrations

Extended connectors and integrations for NoSQL databases, REST APIs, and multi-cloud storage. Expand your data pipeline platform with enterprise-grade connectivity.

## Features

### 1. 🗄️ NoSQL Connectors
- **MongoDB**: Read/write with Spark MongoDB connector
- **Cassandra**: Distributed NoSQL with DataStax connector

### 2. 🌐 REST API Connector
- **HTTP/HTTPS**: Configurable REST API integration
- **Pagination Support**: Auto-paginated API calls
- **All Methods**: GET, POST, PUT, PATCH, DELETE

### 3. ☁️ Cloud Storage Connectors
- **Azure Blob Storage**: Microsoft Azure integration
- **Google Cloud Storage**: GCP integration
- **Multi-Cloud**: Seamless cross-cloud pipelines

## Quick Start

### MongoDB Integration

```java
import com.enterprise.pipeline.integrations.nosql.MongoDBConnector;
import com.enterprise.pipeline.integrations.common.ConnectionConfig;

// Configure connection
ConnectionConfig config = ConnectionConfig.builder()
    .host("localhost")
    .port(27017)
    .database("mydb")
    .collection("customers")
    .username("admin")
    .password("password")
    .build();

// Read from MongoDB
Dataset<Row> data = MongoDBConnector.read(spark, config);

// Write to MongoDB
MongoDBConnector.write(data, config, SaveMode.Append);

// Aggregation pipeline
String pipeline = "[{$match: {age: {$gt: 18}}}, {$group: {_id: '$city', count: {$sum: 1}}}]";
Dataset<Row> aggregated = MongoDBConnector.aggregate(spark, config, pipeline);
```

### Cassandra Integration

```java
import com.enterprise.pipeline.integrations.nosql.CassandraConnector;

// Configure connection
ConnectionConfig config = ConnectionConfig.builder()
    .host("localhost")
    .port(9042)
    .set("keyspace", "myks")
    .table("customers")
    .username("cassandra")
    .password("cassandra")
    .build();

// Read from Cassandra
Dataset<Row> data = CassandraConnector.read(spark, config);

// Write to Cassandra
CassandraConnector.write(data, config, SaveMode.Append);

// Filter pushdown
Dataset<Row> filtered = CassandraConnector.readWithFilter(
    spark, config, "age > 25"
);
```

### REST API Integration

```java
import com.enterprise.pipeline.integrations.rest.RESTAPIConnector;

// Read from REST API
ConnectionConfig config = ConnectionConfig.builder()
    .url("https://api.example.com/users")
    .set("method", "GET")
    .set("headers", Map.of(
        "Authorization", "Bearer token123",
        "Content-Type", "application/json"
    ))
    .build();

Dataset<Row> data = RESTAPIConnector.read(spark, config);

// Write to REST API
ConnectionConfig writeConfig = ConnectionConfig.builder()
    .url("https://api.example.com/users")
    .set("method", "POST")
    .set("batchSize", 100)
    .build();

RESTAPIConnector.write(data, writeConfig, "POST");

// Paginated API
ConnectionConfig paginatedConfig = ConnectionConfig.builder()
    .url("https://api.example.com/users")
    .set("pageParam", "page")
    .set("maxPages", 50)
    .build();

Dataset<Row> allPages = RESTAPIConnector.readPaginated(spark, paginatedConfig);
```

### Azure Blob Storage

```java
import com.enterprise.pipeline.integrations.cloud.AzureBlobConnector;

// Configure Azure connection
ConnectionConfig config = ConnectionConfig.builder()
    .set("accountName", "mystorageaccount")
    .set("accountKey", "your-access-key")
    .set("container", "data")
    .set("blobName", "customers.parquet")
    .build();

// Read from Azure
Dataset<Row> data = AzureBlobConnector.read(spark, config, "parquet");

// Write to Azure
AzureBlobConnector.write(data, config, "parquet", SaveMode.Overwrite);

// List blobs
AzureBlobConnector.listBlobs(config);
```

### Google Cloud Storage

```java
import com.enterprise.pipeline.integrations.cloud.GCSConnector;

// Configure GCS connection
ConnectionConfig config = ConnectionConfig.builder()
    .set("projectId", "my-gcp-project")
    .set("bucket", "my-data-bucket")
    .set("path", "data/customers.parquet")
    .set("credentialsPath", "/path/to/service-account.json")
    .build();

// Read from GCS
Dataset<Row> data = GCSConnector.read(spark, config, "parquet");

// Write to GCS
GCSConnector.write(data, config, "parquet", SaveMode.Overwrite);

// List objects
GCSConnector.listObjects(config);
```

---

## Multi-Cloud Data Pipeline

Build pipelines that span multiple clouds:

```java
// Read from MongoDB
ConnectionConfig mongoConfig = ConnectionConfig.builder()
    .host("mongodb.example.com")
    .database("production")
    .collection("transactions")
    .build();

Dataset<Row> transactions = MongoDBConnector.read(spark, mongoConfig);

// Process data
Dataset<Row> analytics = transactions
    .filter("amount > 1000")
    .groupBy("customer_id")
    .agg(sum("amount").alias("total"));

// Write to Azure
ConnectionConfig azureConfig = ConnectionConfig.builder()
    .set("accountName", "azureaccount")
    .set("accountKey", "key")
    .set("container", "analytics")
    .set("blobName", "daily_totals.parquet")
    .build();

AzureBlobConnector.write(analytics, azureConfig, "parquet", SaveMode.Overwrite);

// Also write to GCS
ConnectionConfig gcsConfig = ConnectionConfig.builder()
    .set("projectId", "gcp-project")
    .set("bucket", "analytics")
    .set("path", "daily_totals.parquet")
    .build();

GCSConnector.write(analytics, gcsConfig, "parquet", SaveMode.Overwrite);
```

---

## Integration with SDK Orchestrator

Use connectors in orchestrated workflows:

```java
public class MongoDBETLJob implements Job {
    @Override
    public JobResult execute(JobContext context) {
        // Read from MongoDB
        ConnectionConfig config = /* configure */;
        Dataset<Row> data = MongoDBConnector.read(spark, config);

        // Transform
        Dataset<Row> transformed = data.filter("active = true");

        // Write to Azure
        AzureBlobConnector.write(transformed, azureConfig, "parquet", SaveMode.Overwrite);

        return JobResult.success(getId());
    }
}

// Use in pipeline
Pipeline.create("mongodb-to-azure")
    .addJob("extract", new MongoDBETLJob())
    .addJob("quality_check", qualityJob)
        .dependsOn("extract")
    .run();
```

---

## Integration with Data Quality Framework

Validate data from external sources:

```java
// Read from MongoDB
Dataset<Row> data = MongoDBConnector.read(spark, mongoConfig);

// Quality assessment
QualityReport report = DataQualityFramework.assess(data, "mongodb_import")
    .profile()
    .withRules(rules -> rules
        .addNotNull("customer_id")
        .addUnique("email")
    )
    .generate();

if (report.passes(90.0)) {
    // Write to Azure if quality passes
    AzureBlobConnector.write(data, azureConfig, "parquet", SaveMode.Overwrite);
}
```

---

## Connection Configuration

`ConnectionConfig` provides a flexible configuration system:

```java
ConnectionConfig config = ConnectionConfig.builder()
    // Standard fields
    .host("hostname")
    .port(1234)
    .username("user")
    .password("pass")
    .database("db")
    .collection("collection")
    .table("table")
    .url("https://api.example.com")

    // Custom fields
    .set("customKey", "customValue")
    .set("timeout", 30000)
    .set("retries", 3)

    .build();

// Access configuration
String host = config.getString("host");
int port = config.getInt("port", 8080);  // With default
boolean ssl = config.getBoolean("ssl", false);
```

---

## Running the Example

```bash
cd sdk-integrations
mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.integrations.examples.IntegrationsExample"
```

---

## Supported Formats

All connectors support multiple data formats:

- **Parquet** (recommended for analytics)
- **CSV**
- **JSON**
- **Avro**
- **ORC**

---

## Dependencies

- MongoDB Spark Connector 10.2.0
- Cassandra Spark Connector 3.4.1
- Apache HttpClient 5.2.1
- Azure Storage Blob 12.23.0
- Google Cloud Storage 2.26.1

---

## Best Practices

1. **Credentials**: Never hardcode credentials, use environment variables or secret managers
2. **Batch Size**: For REST APIs, adjust batch size based on API limits
3. **Error Handling**: Always handle connection failures gracefully
4. **Partitioning**: Use appropriate partitioning for large datasets
5. **Format Selection**: Use Parquet for analytics, JSON for APIs
6. **Multi-Cloud**: Leverage multi-cloud for disaster recovery and redundancy

---

## License

Copyright © 2024 Enterprise Data Pipeline Team
