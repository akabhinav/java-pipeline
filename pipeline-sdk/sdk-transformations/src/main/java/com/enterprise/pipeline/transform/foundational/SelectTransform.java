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
 * Select specific columns from the dataset.
 *
 * Config:
 * - columns: List of column names to select
 *
 * Example:
 * {"type": "select", "config": {"columns": ["id", "name", "age"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class SelectTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(SelectTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "select";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("columns")) {
            throw new ValidationException("'columns' is required");
        }
        Object columns = config.get("columns");
        if (!(columns instanceof List)) {
            throw new ValidationException("'columns' must be a list");
        }
        List<?> columnList = (List<?>) columns;
        if (columnList.isEmpty()) {
            throw new ValidationException("'columns' list cannot be empty");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> columns = (List<String>) config.get("columns");
            logger.debug("Selecting columns: {}", columns);

            return input.select(columns.get(0),
                    columns.stream().skip(1).toArray(String[]::new));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to select columns: " + e.getMessage(), e);
        }
    }
}
