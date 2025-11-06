package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Fill null values with a specified value.
 *
 * Config:
 * - value: Value to fill nulls with
 * - columns: List of columns to fill (optional, fills all if not specified)
 *
 * Example:
 * {"type": "fillNa", "config": {"value": 0, "columns": ["age", "score"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class FillNaTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(FillNaTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "fillNa";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("value")) {
            throw new ValidationException("'value' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            Object value = config.get("value");

            if (config.containsKey("columns")) {
                List<String> columns = (List<String>) config.get("columns");
                logger.debug("Filling nulls in columns {} with value: {}", columns, value);

                // Create a map for column-specific fill (avoids varargs issue)
                Map<String, Object> fillMap = new HashMap<>();
                for (String col : columns) {
                    fillMap.put(col, value);
                }
                return input.na().fill(fillMap);
            } else {
                logger.debug("Filling all nulls with value: {}", value);
                // Type check and cast to appropriate type for fill method
                if (value instanceof String) {
                    return input.na().fill((String) value);
                } else if (value instanceof Double) {
                    return input.na().fill((Double) value);
                } else if (value instanceof Long) {
                    return input.na().fill((Long) value);
                } else if (value instanceof Integer) {
                    return input.na().fill(((Integer) value).longValue());
                } else if (value instanceof Boolean) {
                    return input.na().fill((Boolean) value);
                } else {
                    throw new IllegalArgumentException("Unsupported value type: " + value.getClass().getName());
                }
            }

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to fill nulls: " + e.getMessage(), e);
        }
    }
}
