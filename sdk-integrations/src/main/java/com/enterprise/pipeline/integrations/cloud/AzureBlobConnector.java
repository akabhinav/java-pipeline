package com.enterprise.pipeline.integrations.cloud;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.enterprise.pipeline.integrations.common.ConnectionConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure Blob Storage connector for reading and writing data.
 *
 * <p>Example usage:
 * <pre>{@code
 * ConnectionConfig config = ConnectionConfig.builder()
 *     .set("accountName", "myaccount")
 *     .set("accountKey", "mykey")
 *     .set("container", "mycontainer")
 *     .set("blobName", "data.parquet")
 *     .build();
 *
 * Dataset<Row> data = AzureBlobConnector.read(spark, config, "parquet");
 * AzureBlobConnector.write(data, config, "parquet", SaveMode.Overwrite);
 * }</pre>
 */
public class AzureBlobConnector {
    private static final Logger logger = LoggerFactory.getLogger(AzureBlobConnector.class);

    /**
     * Read data from Azure Blob Storage.
     */
    public static Dataset<Row> read(SparkSession spark, ConnectionConfig config, String format) {
        String container = config.getString("container");
        String blobName = config.getString("blobName");

        logger.info("Reading from Azure Blob: {}/{}", container, blobName);

        // Configure Azure credentials
        configureAzure(spark, config);

        // Build Azure path
        String path = buildAzurePath(config);

        Dataset<Row> dataset = spark.read()
            .format(format)
            .load(path);

        logger.info("Successfully read {} rows from Azure Blob", dataset.count());
        return dataset;
    }

    /**
     * Write data to Azure Blob Storage.
     */
    public static void write(Dataset<Row> dataset, ConnectionConfig config,
                            String format, SaveMode saveMode) {
        String container = config.getString("container");
        String blobName = config.getString("blobName");

        logger.info("Writing to Azure Blob: {}/{}", container, blobName);

        // Configure Azure credentials
        configureAzure(dataset.sparkSession(), config);

        // Build Azure path
        String path = buildAzurePath(config);

        dataset.write()
            .format(format)
            .mode(saveMode)
            .save(path);

        logger.info("Successfully wrote data to Azure Blob");
    }

    /**
     * Configure Azure credentials in Spark.
     */
    private static void configureAzure(SparkSession spark, ConnectionConfig config) {
        String accountName = config.getString("accountName");
        String accountKey = config.getString("accountKey");

        String confKey = "fs.azure.account.key." + accountName + ".blob.core.windows.net";
        spark.conf().set(confKey, accountKey);
    }

    /**
     * Build Azure Blob Storage path.
     */
    private static String buildAzurePath(ConnectionConfig config) {
        String accountName = config.getString("accountName");
        String container = config.getString("container");
        String blobName = config.getString("blobName");

        return String.format("wasbs://%s@%s.blob.core.windows.net/%s",
            container, accountName, blobName);
    }

    /**
     * List blobs in container.
     */
    public static void listBlobs(ConnectionConfig config) {
        logger.info("Listing blobs in container: {}", config.getString("container"));

        BlobServiceClient serviceClient = createServiceClient(config);
        BlobContainerClient containerClient = serviceClient.getBlobContainerClient(
            config.getString("container"));

        containerClient.listBlobs().forEach(blob ->
            logger.info("  - {} (size: {} bytes)", blob.getName(), blob.getProperties().getContentLength())
        );
    }

    /**
     * Create Azure Blob Service client.
     */
    private static BlobServiceClient createServiceClient(ConnectionConfig config) {
        String accountName = config.getString("accountName");
        String accountKey = config.getString("accountKey");

        String connectionString = String.format(
            "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
            accountName, accountKey
        );

        return new BlobServiceClientBuilder()
            .connectionString(connectionString)
            .buildClient();
    }
}
