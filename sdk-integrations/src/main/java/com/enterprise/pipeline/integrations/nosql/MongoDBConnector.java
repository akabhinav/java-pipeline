package com.enterprise.pipeline.integrations.nosql;

import com.enterprise.pipeline.integrations.common.ConnectionConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * MongoDB connector for reading and writing data using Spark MongoDB connector.
 *
 * <p>Example usage:
 * <pre>{@code
 * ConnectionConfig config = ConnectionConfig.builder()
 *     .host("localhost")
 *     .port(27017)
 *     .database("mydb")
 *     .collection("customers")
 *     .username("user")
 *     .password("pass")
 *     .build();
 *
 * Dataset<Row> data = MongoDBConnector.read(spark, config);
 * MongoDBConnector.write(data, config, SaveMode.Append);
 * }</pre>
 */
public class MongoDBConnector {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBConnector.class);

    /**
     * Read data from MongoDB collection.
     */
    public static Dataset<Row> read(SparkSession spark, ConnectionConfig config) {
        logger.info("Reading from MongoDB: {}.{}",
            config.getString("database"), config.getString("collection"));

        String uri = buildMongoURI(config);

        Map<String, String> options = new HashMap<>();
        options.put("uri", uri);
        options.put("database", config.getString("database"));
        options.put("collection", config.getString("collection"));

        // Optional: Add pipeline for server-side filtering
        if (config.containsKey("pipeline")) {
            options.put("pipeline", config.getString("pipeline"));
        }

        // Optional: Sample size for schema inference
        if (config.containsKey("sampleSize")) {
            options.put("sampleSize", config.getString("sampleSize", "1000"));
        }

        Dataset<Row> dataset = spark.read()
            .format("mongodb")
            .options(options)
            .load();

        logger.info("Successfully read {} rows from MongoDB", dataset.count());
        return dataset;
    }

    /**
     * Write data to MongoDB collection.
     */
    public static void write(Dataset<Row> dataset, ConnectionConfig config, SaveMode saveMode) {
        logger.info("Writing to MongoDB: {}.{}",
            config.getString("database"), config.getString("collection"));

        String uri = buildMongoURI(config);

        Map<String, String> options = new HashMap<>();
        options.put("uri", uri);
        options.put("database", config.getString("database"));
        options.put("collection", config.getString("collection"));

        // Optional: Set write concern
        if (config.containsKey("writeConcern")) {
            options.put("writeConcern.w", config.getString("writeConcern", "majority"));
        }

        dataset.write()
            .format("mongodb")
            .options(options)
            .mode(saveMode)
            .save();

        logger.info("Successfully wrote data to MongoDB");
    }

    /**
     * Build MongoDB connection URI.
     */
    private static String buildMongoURI(ConnectionConfig config) {
        StringBuilder uri = new StringBuilder("mongodb://");

        // Add authentication if provided
        if (config.containsKey("username") && config.containsKey("password")) {
            uri.append(config.getString("username"))
               .append(":")
               .append(config.getString("password"))
               .append("@");
        }

        // Add host and port
        String host = config.getString("host", "localhost");
        int port = config.getInt("port", 27017);
        uri.append(host).append(":").append(port);

        // Add auth database if specified
        if (config.containsKey("authSource")) {
            uri.append("/?authSource=").append(config.getString("authSource"));
        }

        return uri.toString();
    }

    /**
     * Execute aggregation pipeline on MongoDB.
     */
    public static Dataset<Row> aggregate(SparkSession spark, ConnectionConfig config, String pipeline) {
        config.set("pipeline", pipeline);
        return read(spark, config);
    }

    /**
     * Read with custom options.
     */
    public static Dataset<Row> readWithOptions(SparkSession spark, ConnectionConfig config,
                                               Map<String, String> additionalOptions) {
        String uri = buildMongoURI(config);

        Map<String, String> options = new HashMap<>();
        options.put("uri", uri);
        options.put("database", config.getString("database"));
        options.put("collection", config.getString("collection"));
        options.putAll(additionalOptions);

        return spark.read()
            .format("mongodb")
            .options(options)
            .load();
    }
}
