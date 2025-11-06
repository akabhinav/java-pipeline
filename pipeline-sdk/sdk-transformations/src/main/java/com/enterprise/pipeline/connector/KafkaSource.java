package com.enterprise.pipeline.connector;

import com.enterprise.pipeline.api.Source;
import com.enterprise.pipeline.api.SourceException;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Apache Kafka source connector.
 * Supports reading streaming or batch data from Kafka topics.
 *
 * Config:
 * - bootstrapServers: Kafka bootstrap servers (required)
 *   Example: "localhost:9092" or "broker1:9092,broker2:9092"
 * - topic: Kafka topic name (required for single topic)
 * - topics: Comma-separated list of topics (optional, for multiple topics)
 * - topicPattern: Regex pattern for topic subscription (optional)
 * - startingOffsets: Where to start reading (optional, default: latest)
 *   Options: "earliest", "latest", or JSON offset specification
 * - endingOffsets: Where to stop reading for batch (optional, default: latest)
 * - maxOffsetsPerTrigger: Max records per batch in streaming (optional)
 * - failOnDataLoss: Fail query when data loss detected (optional, default: true)
 * - groupId: Consumer group ID (optional)
 * - options: Additional Kafka options (optional)
 *
 * Example (single topic):
 * {
 *   "type": "kafka",
 *   "config": {
 *     "bootstrapServers": "localhost:9092",
 *     "topic": "transactions",
 *     "startingOffsets": "earliest"
 *   }
 * }
 *
 * Example (multiple topics):
 * {
 *   "type": "kafka",
 *   "config": {
 *     "bootstrapServers": "broker1:9092,broker2:9092",
 *     "topics": "topic1,topic2,topic3",
 *     "startingOffsets": "latest"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class KafkaSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSource.class);
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

        // At least one of topic, topics, or topicPattern must be specified
        boolean hasTopic = config.containsKey("topic");
        boolean hasTopics = config.containsKey("topics");
        boolean hasPattern = config.containsKey("topicPattern");

        if (!hasTopic && !hasTopics && !hasPattern) {
            throw new ValidationException(
                    "At least one of 'topic', 'topics', or 'topicPattern' is required");
        }

        if ((hasTopic && hasTopics) || (hasTopic && hasPattern) || (hasTopics && hasPattern)) {
            throw new ValidationException(
                    "Only one of 'topic', 'topics', or 'topicPattern' can be specified");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> read(Map<String, Object> config, TransformationContext context)
            throws SourceException {
        try {
            String bootstrapServers = (String) config.get("bootstrapServers");

            logger.info("Reading from Kafka: {}", bootstrapServers);

            SparkSession spark = context.getSparkSession();

            // Start with basic Kafka configuration
            var reader = spark.read()
                    .format("kafka")
                    .option("kafka.bootstrap.servers", bootstrapServers);

            // Configure topic subscription
            if (config.containsKey("topic")) {
                String topic = (String) config.get("topic");
                reader = reader.option("subscribe", topic);
                logger.debug("Subscribing to topic: {}", topic);
            } else if (config.containsKey("topics")) {
                String topics = (String) config.get("topics");
                reader = reader.option("subscribe", topics);
                logger.debug("Subscribing to topics: {}", topics);
            } else if (config.containsKey("topicPattern")) {
                String topicPattern = (String) config.get("topicPattern");
                reader = reader.option("subscribePattern", topicPattern);
                logger.debug("Subscribing to topic pattern: {}", topicPattern);
            }

            // Configure offsets
            if (config.containsKey("startingOffsets")) {
                String startingOffsets = String.valueOf(config.get("startingOffsets"));
                reader = reader.option("startingOffsets", startingOffsets);
            } else {
                reader = reader.option("startingOffsets", "latest");
            }

            if (config.containsKey("endingOffsets")) {
                String endingOffsets = String.valueOf(config.get("endingOffsets"));
                reader = reader.option("endingOffsets", endingOffsets);
            }

            // Configure additional options
            if (config.containsKey("failOnDataLoss")) {
                Boolean failOnDataLoss = (Boolean) config.get("failOnDataLoss");
                reader = reader.option("failOnDataLoss", failOnDataLoss.toString());
            }

            if (config.containsKey("groupId")) {
                String groupId = (String) config.get("groupId");
                reader = reader.option("kafka.group.id", groupId);
            }

            if (config.containsKey("maxOffsetsPerTrigger")) {
                Object maxOffsets = config.get("maxOffsetsPerTrigger");
                reader = reader.option("maxOffsetsPerTrigger", String.valueOf(maxOffsets));
            }

            // Apply additional Kafka options if provided
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    String key = option.getKey().startsWith("kafka.")
                            ? option.getKey()
                            : "kafka." + option.getKey();
                    reader = reader.option(key, option.getValue());
                }
            }

            Dataset<Row> rawData = reader.load();

            logger.info("Successfully connected to Kafka");
            logger.debug("Kafka schema: {}", rawData.schema());

            // The DataFrame will have columns: key, value, topic, partition, offset, timestamp, timestampType
            // User can transform the 'value' column as needed

            return rawData;

        } catch (Exception e) {
            throw new SourceException(getName(),
                    "Failed to read from Kafka: " + e.getMessage(), e);
        }
    }
}
