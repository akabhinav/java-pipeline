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
 * Rename a column.
 *
 * Config:
 * - oldName: Current column name
 * - newName: New column name
 *
 * Example:
 * {"type": "renameColumn", "config": {"oldName": "customer_id", "newName": "cust_id"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class RenameColumnTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(RenameColumnTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "renameColumn";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("oldName")) {
            throw new ValidationException("'oldName' is required");
        }
        if (!config.containsKey("newName")) {
            throw new ValidationException("'newName' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String oldName = (String) config.get("oldName");
            String newName = (String) config.get("newName");

            logger.debug("Renaming column '{}' to '{}'", oldName, newName);

            return input.withColumnRenamed(oldName, newName);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to rename column: " + e.getMessage(), e);
        }
    }
}
