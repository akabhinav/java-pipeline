package com.enterprise.pipeline.integrations.examples;

import com.enterprise.pipeline.integrations.cloud.AzureBlobConnector;
import com.enterprise.pipeline.integrations.cloud.GCSConnector;
import com.enterprise.pipeline.integrations.common.ConnectionConfig;
import com.enterprise.pipeline.integrations.nosql.CassandraConnector;
import com.enterprise.pipeline.integrations.nosql.MongoDBConnector;
import com.enterprise.pipeline.integrations.rest.RESTAPIConnector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Comprehensive example demonstrating all integration connectors.
 */
public class IntegrationsExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("SDK Integrations Example")
            .master("local[*]")
            .getOrCreate();

        try {
            System.out.println("\n" + "=".repeat(100));
            System.out.println("SDK INTEGRATIONS - COMPREHENSIVE EXAMPLE");
            System.out.println("=".repeat(100) + "\n");

            // Example 1: MongoDB Integration
            example1MongoDB(spark);

            // Example 2: Cassandra Integration
            example2Cassandra(spark);

            // Example 3: REST API Integration
            example3RestAPI(spark);

            // Example 4: Azure Blob Storage
            example4Azure(spark);

            // Example 5: Google Cloud Storage
            example5GCS(spark);

            // Example 6: Multi-Cloud Data Pipeline
            example6MultiCloud(spark);

            System.out.println("\n" + "=".repeat(100));
            System.out.println("ALL EXAMPLES COMPLETED SUCCESSFULLY");
            System.out.println("=".repeat(100) + "\n");

        } finally {
            spark.stop();
        }
    }

    private static void example1MongoDB(SparkSession spark) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("EXAMPLE 1: MongoDB Integration");
        System.out.println("=".repeat(100));

        try {
            ConnectionConfig config = ConnectionConfig.builder()
                .host("localhost")
                .port(27017)
                .database("testdb")
                .collection("customers")
                .username("admin")
                .password("password")
                .build();

            // Read from MongoDB
            System.out.println("\nReading from MongoDB...");
            Dataset<Row> data = MongoDBConnector.read(spark, config);
            data.show(10, false);

            // Write to MongoDB
            System.out.println("\nWriting to MongoDB...");
            MongoDBConnector.write(data, config, SaveMode.Overwrite);

            System.out.println("✓ MongoDB integration completed successfully");

        } catch (Exception e) {
            System.out.println("✗ MongoDB example skipped (connection not available): " + e.getMessage());
        }
    }

    private static void example2Cassandra(SparkSession spark) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("EXAMPLE 2: Cassandra Integration");
        System.out.println("=".repeat(100));

        try {
            ConnectionConfig config = ConnectionConfig.builder()
                .host("localhost")
                .port(9042)
                .set("keyspace", "myks")
                .table("customers")
                .username("cassandra")
                .password("cassandra")
                .build();

            // Read from Cassandra
            System.out.println("\nReading from Cassandra...");
            Dataset<Row> data = CassandraConnector.read(spark, config);
            data.show(10, false);

            // Write to Cassandra
            System.out.println("\nWriting to Cassandra...");
            CassandraConnector.write(data, config, SaveMode.Append);

            System.out.println("✓ Cassandra integration completed successfully");

        } catch (Exception e) {
            System.out.println("✗ Cassandra example skipped (connection not available): " + e.getMessage());
        }
    }

    private static void example3RestAPI(SparkSession spark) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("EXAMPLE 3: REST API Integration");
        System.out.println("=".repeat(100));

        try {
            // Read from public REST API
            ConnectionConfig config = ConnectionConfig.builder()
                .url("https://jsonplaceholder.typicode.com/users")
                .set("method", "GET")
                .build();

            System.out.println("\nReading from REST API...");
            Dataset<Row> data = RESTAPIConnector.read(spark, config);
            data.show(10, false);

            // Write to REST API (POST)
            ConnectionConfig writeConfig = ConnectionConfig.builder()
                .url("https://jsonplaceholder.typicode.com/posts")
                .set("method", "POST")
                .set("batchSize", 10)
                .build();

            System.out.println("\nWriting to REST API...");
            RESTAPIConnector.write(data.limit(5), writeConfig, "POST");

            System.out.println("✓ REST API integration completed successfully");

        } catch (Exception e) {
            System.out.println("✗ REST API example failed: " + e.getMessage());
        }
    }

    private static void example4Azure(SparkSession spark) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("EXAMPLE 4: Azure Blob Storage Integration");
        System.out.println("=".repeat(100));

        try {
            ConnectionConfig config = ConnectionConfig.builder()
                .set("accountName", "myaccount")
                .set("accountKey", "mykey")
                .set("container", "data")
                .set("blobName", "customers.parquet")
                .build();

            // Create sample data
            Dataset<Row> sampleData = createSampleData(spark);

            // Write to Azure
            System.out.println("\nWriting to Azure Blob Storage...");
            AzureBlobConnector.write(sampleData, config, "parquet", SaveMode.Overwrite);

            // Read from Azure
            System.out.println("\nReading from Azure Blob Storage...");
            Dataset<Row> data = AzureBlobConnector.read(spark, config, "parquet");
            data.show(10, false);

            System.out.println("✓ Azure Blob Storage integration completed successfully");

        } catch (Exception e) {
            System.out.println("✗ Azure example skipped (credentials not configured): " + e.getMessage());
        }
    }

    private static void example5GCS(SparkSession spark) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("EXAMPLE 5: Google Cloud Storage Integration");
        System.out.println("=".repeat(100));

        try {
            ConnectionConfig config = ConnectionConfig.builder()
                .set("projectId", "my-project")
                .set("bucket", "my-bucket")
                .set("path", "data/customers.parquet")
                .set("credentialsPath", "/path/to/credentials.json")
                .build();

            // Create sample data
            Dataset<Row> sampleData = createSampleData(spark);

            // Write to GCS
            System.out.println("\nWriting to Google Cloud Storage...");
            GCSConnector.write(sampleData, config, "parquet", SaveMode.Overwrite);

            // Read from GCS
            System.out.println("\nReading from Google Cloud Storage...");
            Dataset<Row> data = GCSConnector.read(spark, config, "parquet");
            data.show(10, false);

            System.out.println("✓ Google Cloud Storage integration completed successfully");

        } catch (Exception e) {
            System.out.println("✗ GCS example skipped (credentials not configured): " + e.getMessage());
        }
    }

    private static void example6MultiCloud(SparkSession spark) {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("EXAMPLE 6: Multi-Cloud Data Pipeline");
        System.out.println("=".repeat(100));

        System.out.println("\nScenario: Read from MongoDB, process, write to Azure and GCS");

        try {
            // Step 1: Read from MongoDB
            ConnectionConfig mongoConfig = ConnectionConfig.builder()
                .host("localhost")
                .port(27017)
                .database("production")
                .collection("transactions")
                .build();

            System.out.println("\n1. Reading from MongoDB...");
            Dataset<Row> data = MongoDBConnector.read(spark, mongoConfig);

            // Step 2: Process data
            System.out.println("\n2. Processing data...");
            Dataset<Row> processed = data
                .filter("amount > 100")
                .groupBy("customer_id")
                .agg(org.apache.spark.sql.functions.sum("amount").alias("total_amount"));

            // Step 3: Write to Azure
            ConnectionConfig azureConfig = ConnectionConfig.builder()
                .set("accountName", "myaccount")
                .set("accountKey", "mykey")
                .set("container", "analytics")
                .set("blobName", "customer_totals.parquet")
                .build();

            System.out.println("\n3. Writing to Azure Blob Storage...");
            AzureBlobConnector.write(processed, azureConfig, "parquet", SaveMode.Overwrite);

            // Step 4: Write to GCS
            ConnectionConfig gcsConfig = ConnectionConfig.builder()
                .set("projectId", "my-project")
                .set("bucket", "analytics")
                .set("path", "customer_totals.parquet")
                .build();

            System.out.println("\n4. Writing to Google Cloud Storage...");
            GCSConnector.write(processed, gcsConfig, "parquet", SaveMode.Overwrite);

            System.out.println("\n✓ Multi-cloud pipeline completed successfully");
            System.out.println("  - Read from: MongoDB");
            System.out.println("  - Wrote to: Azure Blob Storage + Google Cloud Storage");

        } catch (Exception e) {
            System.out.println("✗ Multi-cloud example skipped: " + e.getMessage());
        }
    }

    private static Dataset<Row> createSampleData(SparkSession spark) {
        return spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("data:text/csv," +
                "id,name,email,age,city\n" +
                "1,John Doe,john@example.com,35,New York\n" +
                "2,Jane Smith,jane@example.com,28,Los Angeles\n" +
                "3,Bob Johnson,bob@example.com,42,Chicago\n"
            );
    }
}
