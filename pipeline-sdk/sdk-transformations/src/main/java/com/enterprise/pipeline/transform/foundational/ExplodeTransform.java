package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Explode an array or map column into multiple rows.
 *
 * Config:
 * - column: Array or map column to explode
 * - alias: Optional alias for the exploded column (default: uses original name)
 *
 * Example:
 * {"type": "explode", "config": {"column": "tags", "alias": "tag"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class ExplodeTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ExplodeTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "explode";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("column")) {
            throw new ValidationException("'column' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String column = (String) config.get("column");
            String alias = config.containsKey("alias") ? (String) config.get("alias") : column;

            logger.debug("Exploding column '{}' as '{}'", column, alias);

            return input.withColumn(alias, functions.explode(functions.col(column)));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to explode column: " + e.getMessage(), e);
        }
    }
}
