package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Repartition the dataset to the specified number of partitions.
 * Optionally partition by columns.
 *
 * Config:
 * - numPartitions: Target number of partitions
 * - columns: Optional list of columns to partition by
 *
 * Example:
 * {"type": "repartition", "config": {"numPartitions": 10, "columns": ["customer_id"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class RepartitionTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(RepartitionTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "repartition";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("numPartitions")) {
            throw new ValidationException("'numPartitions' is required");
        }
        Object numPartitions = config.get("numPartitions");
        if (!(numPartitions instanceof Number)) {
            throw new ValidationException("'numPartitions' must be a number");
        }
        if (((Number) numPartitions).intValue() < 1) {
            throw new ValidationException("'numPartitions' must be at least 1");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            int numPartitions = ((Number) config.get("numPartitions")).intValue();

            if (config.containsKey("columns")) {
                List<String> columns = (List<String>) config.get("columns");
                logger.debug("Repartitioning to {} partitions by columns: {}", numPartitions, columns);
                org.apache.spark.sql.Column[] colArray = columns.stream()
                        .map(org.apache.spark.sql.functions::col)
                        .toArray(org.apache.spark.sql.Column[]::new);
                return input.repartition(numPartitions, colArray);
            } else {
                logger.debug("Repartitioning to {} partitions", numPartitions);
                return input.repartition(numPartitions);
            }

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to repartition: " + e.getMessage(), e);
        }
    }
}
