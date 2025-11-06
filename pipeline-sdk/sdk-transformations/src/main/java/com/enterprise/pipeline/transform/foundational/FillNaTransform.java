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
                return input.na().fill(value, columns.toArray(new String[0]));
            } else {
                logger.debug("Filling all nulls with value: {}", value);
                return input.na().fill(value);
            }

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to fill nulls: " + e.getMessage(), e);
        }
    }
}
