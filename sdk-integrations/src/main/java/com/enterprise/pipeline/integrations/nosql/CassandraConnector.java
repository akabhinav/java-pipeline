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
 * Cassandra connector for reading and writing data using Spark Cassandra connector.
 *
 * <p>Example usage:
 * <pre>{@code
 * ConnectionConfig config = ConnectionConfig.builder()
 *     .host("localhost")
 *     .port(9042)
 *     .set("keyspace", "myks")
 *     .table("customers")
 *     .username("cassandra")
 *     .password("cassandra")
 *     .build();
 *
 * Dataset<Row> data = CassandraConnector.read(spark, config);
 * CassandraConnector.write(data, config, SaveMode.Append);
 * }</pre>
 */
public class CassandraConnector {
    private static final Logger logger = LoggerFactory.getLogger(CassandraConnector.class);

    /**
     * Read data from Cassandra table.
     */
    public static Dataset<Row> read(SparkSession spark, ConnectionConfig config) {
        String keyspace = config.getString("keyspace");
        String table = config.getString("table");

        logger.info("Reading from Cassandra: {}.{}", keyspace, table);

        Map<String, String> options = buildOptions(config);

        Dataset<Row> dataset = spark.read()
            .format("org.apache.spark.sql.cassandra")
            .options(options)
            .load();

        logger.info("Successfully read {} rows from Cassandra", dataset.count());
        return dataset;
    }

    /**
     * Write data to Cassandra table.
     */
    public static void write(Dataset<Row> dataset, ConnectionConfig config, SaveMode saveMode) {
        String keyspace = config.getString("keyspace");
        String table = config.getString("table");

        logger.info("Writing to Cassandra: {}.{}", keyspace, table);

        Map<String, String> options = buildOptions(config);

        dataset.write()
            .format("org.apache.spark.sql.cassandra")
            .options(options)
            .mode(saveMode)
            .save();

        logger.info("Successfully wrote data to Cassandra");
    }

    /**
     * Read with CQL filter pushdown.
     */
    public static Dataset<Row> readWithFilter(SparkSession spark, ConnectionConfig config, String whereClause) {
        config.set("pushdown", "true");
        Dataset<Row> dataset = read(spark, config);
        return dataset.filter(whereClause);
    }

    /**
     * Build Cassandra connection options.
     */
    private static Map<String, String> buildOptions(ConnectionConfig config) {
        Map<String, String> options = new HashMap<>();

        options.put("spark.cassandra.connection.host", config.getString("host", "localhost"));
        options.put("spark.cassandra.connection.port", String.valueOf(config.getInt("port", 9042)));
        options.put("keyspace", config.getString("keyspace"));
        options.put("table", config.getString("table"));

        // Authentication
        if (config.containsKey("username") && config.containsKey("password")) {
            options.put("spark.cassandra.auth.username", config.getString("username"));
            options.put("spark.cassandra.auth.password", config.getString("password"));
        }

        // Optional: Consistency level
        if (config.containsKey("consistencyLevel")) {
            options.put("spark.cassandra.input.consistency.level",
                config.getString("consistencyLevel", "LOCAL_ONE"));
        }

        // Optional: Page row size
        if (config.containsKey("pageRowSize")) {
            options.put("spark.cassandra.input.fetch.size_in_rows",
                config.getString("pageRowSize", "1000"));
        }

        return options;
    }
}
