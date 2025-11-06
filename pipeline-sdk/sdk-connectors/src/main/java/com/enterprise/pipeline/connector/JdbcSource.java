package com.enterprise.pipeline.connector;

import com.enterprise.pipeline.api.Source;
import com.enterprise.pipeline.api.SourceException;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * JDBC source connector.
 * Reads data from relational databases via JDBC.
 *
 * Config:
 * - url: JDBC connection URL (e.g., jdbc:postgresql://localhost:5432/mydb)
 * - table: Table name or SQL query (use dbtable or query)
 * - dbtable: Table name (e.g., "public.customers")
 * - query: SQL query (e.g., "(SELECT * FROM customers WHERE active = true) AS subq")
 * - user: Database username (optional)
 * - password: Database password (optional)
 * - driver: JDBC driver class (optional, auto-detected for common databases)
 * - numPartitions: Number of partitions for parallel reads (optional)
 * - partitionColumn: Column to partition on (optional, for parallel reads)
 * - lowerBound: Lower bound for partitioning (optional)
 * - upperBound: Upper bound for partitioning (optional)
 * - fetchSize: JDBC fetch size (optional)
 * - options: Additional JDBC options (optional)
 *
 * Example:
 * {
 *   "type": "jdbc",
 *   "config": {
 *     "url": "jdbc:postgresql://localhost:5432/banking",
 *     "dbtable": "transactions",
 *     "user": "admin",
 *     "password": "secret",
 *     "numPartitions": 10,
 *     "partitionColumn": "transaction_id",
 *     "lowerBound": "0",
 *     "upperBound": "1000000"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class JdbcSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);
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
        if (!config.containsKey("dbtable") && !config.containsKey("query")) {
            throw new ValidationException("Either 'dbtable' or 'query' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> read(Map<String, Object> config, TransformationContext context)
            throws SourceException {
        try {
            String url = (String) config.get("url");
            String table = config.containsKey("dbtable")
                    ? (String) config.get("dbtable")
                    : (String) config.get("query");

            logger.info("Reading from JDBC source: {} table: {}", url, table);

            DataFrameReader reader = context.getSparkSession().read().format("jdbc");

            // Required properties
            Properties connectionProperties = new Properties();
            reader = reader.option("url", url);
            reader = reader.option(config.containsKey("dbtable") ? "dbtable" : "query", table);

            // Optional authentication
            if (config.containsKey("user")) {
                String user = (String) config.get("user");
                reader = reader.option("user", user);
                connectionProperties.setProperty("user", user);
            }
            if (config.containsKey("password")) {
                String password = (String) config.get("password");
                reader = reader.option("password", password);
                connectionProperties.setProperty("password", password);
            }

            // Optional driver
            if (config.containsKey("driver")) {
                String driver = (String) config.get("driver");
                reader = reader.option("driver", driver);
            }

            // Parallel read options
            if (config.containsKey("numPartitions")) {
                reader = reader.option("numPartitions",
                        String.valueOf(config.get("numPartitions")));
            }
            if (config.containsKey("partitionColumn")) {
                reader = reader.option("partitionColumn",
                        (String) config.get("partitionColumn"));
            }
            if (config.containsKey("lowerBound")) {
                reader = reader.option("lowerBound",
                        String.valueOf(config.get("lowerBound")));
            }
            if (config.containsKey("upperBound")) {
                reader = reader.option("upperBound",
                        String.valueOf(config.get("upperBound")));
            }

            // Fetch size
            if (config.containsKey("fetchSize")) {
                reader = reader.option("fetchsize",
                        String.valueOf(config.get("fetchSize")));
            }

            // Additional options
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    reader = reader.option(option.getKey(), option.getValue());
                }
            }

            return reader.load();

        } catch (Exception e) {
            throw new SourceException(getName(),
                    "Failed to read from JDBC source: " + e.getMessage(), e);
        }
    }
}
