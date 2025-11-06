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

/**
 * Apache Kafka sink connector.
 * Supports writing batch or streaming data to Kafka topics.
 *
 * Config:
 * - bootstrapServers: Kafka bootstrap servers (required)
 *   Example: "localhost:9092" or "broker1:9092,broker2:9092"
 * - topic: Target Kafka topic name (required)
 * - keyColumn: Column to use as Kafka message key (optional)
 *   If not specified, messages will have null keys
 * - valueColumn: Column to use as Kafka message value (required if not using all columns)
 *   Must be string or binary type
 * - options: Additional Kafka options (optional)
 *   Example: compression.type, acks, retries, etc.
 *
 * Note: The DataFrame must have columns that match Kafka's expected schema:
 * - If keyColumn is specified: that column becomes the key
 * - If valueColumn is specified: that column becomes the value
 * - Otherwise, the DataFrame should have 'key' and 'value' columns
 *
 * Example (simple):
 * {
 *   "type": "kafka",
 *   "config": {
 *     "bootstrapServers": "localhost:9092",
 *     "topic": "processed-transactions",
 *     "valueColumn": "json_payload"
 *   }
 * }
 *
 * Example (with key):
 * {
 *   "type": "kafka",
 *   "config": {
 *     "bootstrapServers": "broker1:9092,broker2:9092",
 *     "topic": "customer-events",
 *     "keyColumn": "customer_id",
 *     "valueColumn": "event_data",
 *     "options": {
 *       "compression.type": "gzip",
 *       "acks": "all"
 *     }
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class KafkaSink implements Sink {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "kafka";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("bootstrapServers")) {
            throw new ValidationException("'bootstrapServers' is required");
        }
        if (!config.containsKey("topic")) {
            throw new ValidationException("'topic' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> dataset, Map<String, Object> config, TransformationContext context)
            throws SinkException {
        try {
            String bootstrapServers = (String) config.get("bootstrapServers");
            String topic = (String) config.get("topic");

            logger.info("Writing to Kafka topic '{}' at {}", topic, bootstrapServers);

            // Prepare the DataFrame for Kafka
            Dataset<Row> kafkaData = prepareKafkaData(dataset, config);

            DataFrameWriter<Row> writer = kafkaData.write()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", bootstrapServers)
                    .option("topic", topic);

            // Apply additional Kafka options if provided
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    String key = option.getKey().startsWith("kafka.")
                            ? option.getKey()
                            : "kafka." + option.getKey();
                    writer = writer.option(key, option.getValue());
                    logger.debug("Applied option: {} = {}", key, option.getValue());
                }
            }

            writer.save();
            logger.info("Successfully wrote data to Kafka topic '{}'", topic);

        } catch (Exception e) {
            throw new SinkException(getName(),
                    "Failed to write to Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Prepare DataFrame for Kafka by ensuring it has the required 'value' column
     * and optionally 'key' column.
     */
    private Dataset<Row> prepareKafkaData(Dataset<Row> dataset, Map<String, Object> config) {
        String keyColumn = config.containsKey("keyColumn")
                ? (String) config.get("keyColumn")
                : null;
        String valueColumn = config.containsKey("valueColumn")
                ? (String) config.get("valueColumn")
                : null;

        Dataset<Row> result = dataset;

        // If valueColumn is specified, select or alias it as 'value'
        if (valueColumn != null) {
            if (keyColumn != null) {
                // Both key and value specified
                result = dataset.selectExpr(
                        String.format("CAST(%s AS STRING) AS key", keyColumn),
                        String.format("CAST(%s AS STRING) AS value", valueColumn)
                );
                logger.debug("Using keyColumn='{}' and valueColumn='{}'", keyColumn, valueColumn);
            } else {
                // Only value specified
                result = dataset.selectExpr(
                        String.format("CAST(%s AS STRING) AS value", valueColumn)
                );
                logger.debug("Using valueColumn='{}' (no key)", valueColumn);
            }
        } else {
            // No valueColumn specified, assume DataFrame already has 'value' column
            // and optionally 'key' column
            boolean hasValue = false;
            boolean hasKey = false;

            for (String field : result.columns()) {
                if (field.equals("value")) {
                    hasValue = true;
                }
                if (field.equals("key")) {
                    hasKey = true;
                }
            }

            if (!hasValue) {
                throw new IllegalArgumentException(
                        "DataFrame must have a 'value' column or 'valueColumn' must be specified");
            }

            logger.debug("Using existing 'value' column (hasKey={})", hasKey);
        }

        return result;
    }
}
