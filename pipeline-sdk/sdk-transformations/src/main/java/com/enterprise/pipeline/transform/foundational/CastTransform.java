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
 * Cast a column to a different data type.
 *
 * Config:
 * - column: Column name to cast
 * - targetType: Target data type (string, int, long, double, float, boolean, date, timestamp)
 *
 * Example:
 * {"type": "cast", "config": {"column": "age", "targetType": "int"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class CastTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CastTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "cast";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("column")) {
            throw new ValidationException("'column' is required");
        }
        if (!config.containsKey("targetType")) {
            throw new ValidationException("'targetType' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String column = (String) config.get("column");
            String targetType = (String) config.get("targetType");

            logger.debug("Casting column '{}' to type '{}'", column, targetType);

            return input.withColumn(column,
                org.apache.spark.sql.functions.col(column).cast(targetType));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to cast column: " + e.getMessage(), e);
        }
    }
}
