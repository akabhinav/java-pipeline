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
 * Limit the number of rows in the dataset.
 *
 * Config:
 * - count: Maximum number of rows to keep
 *
 * Example:
 * {"type": "limit", "config": {"count": 1000}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class LimitTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(LimitTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "limit";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("count")) {
            throw new ValidationException("'count' is required");
        }
        Object count = config.get("count");
        if (!(count instanceof Number)) {
            throw new ValidationException("'count' must be a number");
        }
        if (((Number) count).intValue() < 0) {
            throw new ValidationException("'count' must be non-negative");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            int count = ((Number) config.get("count")).intValue();
            logger.debug("Limiting to {} rows", count);

            return input.limit(count);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to limit rows: " + e.getMessage(), e);
        }
    }
}
