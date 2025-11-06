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
 * Add or modify a column using a SQL expression.
 *
 * Config:
 * - columnName: Name of the column to add/modify
 * - expression: SQL expression for the column value
 *
 * Example:
 * {"type": "withColumn", "config": {"columnName": "full_name", "expression": "concat(first_name, ' ', last_name)"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class WithColumnTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(WithColumnTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "withColumn";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("columnName")) {
            throw new ValidationException("'columnName' is required");
        }
        if (!config.containsKey("expression")) {
            throw new ValidationException("'expression' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String columnName = (String) config.get("columnName");
            String expression = (String) config.get("expression");

            logger.debug("Adding/modifying column '{}' with expression: {}", columnName, expression);

            return input.withColumn(columnName, org.apache.spark.sql.functions.expr(expression));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to add/modify column: " + e.getMessage(), e);
        }
    }
}
