package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Reduce the number of partitions to the specified number.
 *
 * Config:
 * - numPartitions: Target number of partitions
 *
 * Example:
 * {"type": "coalesce", "config": {"numPartitions": 1}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class CoalesceTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CoalesceTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "coalesce";
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
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            int numPartitions = ((Number) config.get("numPartitions")).intValue();
            logger.debug("Coalescing to {} partitions", numPartitions);

            return input.coalesce(numPartitions);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to coalesce partitions: " + e.getMessage(), e);
        }
    }
}
