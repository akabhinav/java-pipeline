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
 * Filter rows based on a SQL expression.
 *
 * Config:
 * - condition: SQL WHERE clause expression
 *
 * Example:
 * {"type": "filter", "config": {"condition": "age > 18 AND status = 'active'"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class FilterTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(FilterTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "filter";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("condition")) {
            throw new ValidationException("'condition' is required");
        }
        if (!(config.get("condition") instanceof String)) {
            throw new ValidationException("'condition' must be a string");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String condition = (String) config.get("condition");
            logger.debug("Filtering with condition: {}", condition);

            return input.filter(condition);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to filter rows: " + e.getMessage(), e);
        }
    }
}
