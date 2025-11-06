package com.enterprise.pipeline.connector;

import com.enterprise.pipeline.api.Sink;
import com.enterprise.pipeline.api.SinkException;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * JDBC sink connector.
 * Writes data to relational databases via JDBC.
 *
 * Config:
 * - url: JDBC connection URL (e.g., jdbc:postgresql://localhost:5432/mydb)
 * - dbtable: Target table name (e.g., "public.customers")
 * - user: Database username (optional)
 * - password: Database password (optional)
 * - driver: JDBC driver class (optional, auto-detected for common databases)
 * - mode: Write mode (optional, default: append)
 *   Options: append, overwrite, ignore, error, errorifexists
 * - batchSize: JDBC batch size for inserts (optional, default: 1000)
 * - isolationLevel: Transaction isolation level (optional)
 *   Options: NONE, READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE
 * - truncate: Truncate table before write (optional, only with overwrite mode)
 * - createTableOptions: SQL to append to CREATE TABLE (optional)
 * - createTableColumnTypes: Custom column types for CREATE TABLE (optional)
 * - options: Additional JDBC options (optional)
 *
 * Example:
 * {
 *   "type": "jdbc",
 *   "config": {
 *     "url": "jdbc:postgresql://localhost:5432/banking",
 *     "dbtable": "processed_transactions",
 *     "user": "admin",
 *     "password": "secret",
 *     "mode": "append",
 *     "batchSize": "5000",
 *     "isolationLevel": "READ_COMMITTED"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class JdbcSink implements Sink {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSink.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "jdbc";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("url")) {
            throw new ValidationException("'url' is required");
        }
        if (!config.containsKey("dbtable")) {
            throw new ValidationException("'dbtable' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> dataset, Map<String, Object> config, TransformationContext context)
            throws SinkException {
        try {
            String url = (String) config.get("url");
            String table = (String) config.get("dbtable");
            String mode = config.containsKey("mode") ? (String) config.get("mode") : "append";

            logger.info("Writing to JDBC sink: {} table: {} (mode: {})", url, table, mode);

            DataFrameWriter<Row> writer = dataset.write().format("jdbc").mode(mode);

            // Required properties
            Properties connectionProperties = new Properties();
            writer = writer.option("url", url);
            writer = writer.option("dbtable", table);

            // Optional authentication
            if (config.containsKey("user")) {
                String user = (String) config.get("user");
                writer = writer.option("user", user);
                connectionProperties.setProperty("user", user);
            }
            if (config.containsKey("password")) {
                String password = (String) config.get("password");
                writer = writer.option("password", password);
                connectionProperties.setProperty("password", password);
            }

            // Optional driver
            if (config.containsKey("driver")) {
                String driver = (String) config.get("driver");
                writer = writer.option("driver", driver);
            }

            // Batch size
            if (config.containsKey("batchSize")) {
                writer = writer.option("batchsize",
                        String.valueOf(config.get("batchSize")));
            }

            // Isolation level
            if (config.containsKey("isolationLevel")) {
                writer = writer.option("isolationLevel",
                        (String) config.get("isolationLevel"));
            }

            // Truncate option
            if (config.containsKey("truncate")) {
                writer = writer.option("truncate",
                        String.valueOf(config.get("truncate")));
            }

            // Create table options
            if (config.containsKey("createTableOptions")) {
                writer = writer.option("createTableOptions",
                        (String) config.get("createTableOptions"));
            }

            // Create table column types
            if (config.containsKey("createTableColumnTypes")) {
                writer = writer.option("createTableColumnTypes",
                        (String) config.get("createTableColumnTypes"));
            }

            // Additional options
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    writer = writer.option(option.getKey(), option.getValue());
                }
            }

            writer.save();
            logger.info("Successfully wrote {} rows to {}", dataset.count(), table);

        } catch (Exception e) {
            throw new SinkException(getName(),
                    "Failed to write to JDBC sink: " + e.getMessage(), e);
        }
    }
}
