package com.enterprise.pipeline.integrations.cloud;

import com.enterprise.pipeline.integrations.common.ConnectionConfig;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Google Cloud Storage connector for reading and writing data.
 *
 * <p>Example usage:
 * <pre>{@code
 * ConnectionConfig config = ConnectionConfig.builder()
 *     .set("projectId", "my-project")
 *     .set("bucket", "my-bucket")
 *     .set("path", "data/customers.parquet")
 *     .set("credentialsPath", "/path/to/credentials.json")
 *     .build();
 *
 * Dataset<Row> data = GCSConnector.read(spark, config, "parquet");
 * GCSConnector.write(data, config, "parquet", SaveMode.Overwrite);
 * }</pre>
 */
public class GCSConnector {
    private static final Logger logger = LoggerFactory.getLogger(GCSConnector.class);

    /**
     * Read data from Google Cloud Storage.
     */
    public static Dataset<Row> read(SparkSession spark, ConnectionConfig config, String format) {
        String bucket = config.getString("bucket");
        String path = config.getString("path");

        logger.info("Reading from GCS: gs://{}/{}", bucket, path);

        // Configure GCS credentials
        configureGCS(spark, config);

        // Build GCS path
        String gcsPath = buildGCSPath(config);

        Dataset<Row> dataset = spark.read()
            .format(format)
            .load(gcsPath);

        logger.info("Successfully read {} rows from GCS", dataset.count());
        return dataset;
    }

    /**
     * Write data to Google Cloud Storage.
     */
    public static void write(Dataset<Row> dataset, ConnectionConfig config,
                            String format, SaveMode saveMode) {
        String bucket = config.getString("bucket");
        String path = config.getString("path");

        logger.info("Writing to GCS: gs://{}/{}", bucket, path);

        // Configure GCS credentials
        configureGCS(dataset.sparkSession(), config);

        // Build GCS path
        String gcsPath = buildGCSPath(config);

        dataset.write()
            .format(format)
            .mode(saveMode)
            .save(gcsPath);

        logger.info("Successfully wrote data to GCS");
    }

    /**
     * Configure GCS credentials in Spark.
     */
    private static void configureGCS(SparkSession spark, ConnectionConfig config) {
        // Option 1: Use service account key file
        if (config.containsKey("credentialsPath")) {
            String credentialsPath = config.getString("credentialsPath");
            spark.conf().set("google.cloud.auth.service.account.json.keyfile", credentialsPath);
        }

        // Option 2: Use project ID for default credentials
        if (config.containsKey("projectId")) {
            spark.conf().set("fs.gs.project.id", config.getString("projectId"));
        }
    }

    /**
     * Build GCS path.
     */
    private static String buildGCSPath(ConnectionConfig config) {
        String bucket = config.getString("bucket");
        String path = config.getString("path");
        return String.format("gs://%s/%s", bucket, path);
    }

    /**
     * List objects in bucket.
     */
    public static void listObjects(ConnectionConfig config) {
        String bucket = config.getString("bucket");
        String prefix = config.getString("prefix", "");

        logger.info("Listing objects in bucket: {} with prefix: {}", bucket, prefix);

        Storage storage = StorageOptions.getDefaultInstance().getService();

        storage.list(bucket, Storage.BlobListOption.prefix(prefix))
            .iterateAll()
            .forEach(blob ->
                logger.info("  - {} (size: {} bytes)", blob.getName(), blob.getSize())
            );
    }
}
