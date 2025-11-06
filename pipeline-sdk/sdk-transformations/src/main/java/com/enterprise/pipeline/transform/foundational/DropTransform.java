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
 * Drop specific columns from the dataset.
 *
 * Config:
 * - columns: List of column names to drop
 *
 * Example:
 * {"type": "drop", "config": {"columns": ["temp_col", "internal_id"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class DropTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(DropTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "drop";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("columns")) {
            throw new ValidationException("'columns' is required");
        }
        if (!(config.get("columns") instanceof List)) {
            throw new ValidationException("'columns' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> columns = (List<String>) config.get("columns");
            logger.debug("Dropping columns: {}", columns);

            // Convert list to array for varargs (String... colNames)
            if (columns.isEmpty()) {
                return input;
            }

            String[] colArray = columns.toArray(new String[0]);
            return input.drop(colArray);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to drop columns: " + e.getMessage(), e);
        }
    }
}
