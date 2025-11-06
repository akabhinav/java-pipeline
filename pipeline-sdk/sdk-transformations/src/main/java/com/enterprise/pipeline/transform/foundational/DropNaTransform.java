package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Drop rows with null values.
 *
 * Config:
 * - how: "any" (drop if any null) or "all" (drop if all nulls) - default: "any"
 * - columns: List of columns to check (optional, checks all if not specified)
 *
 * Example:
 * {"type": "dropNa", "config": {"how": "any", "columns": ["name", "email"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class DropNaTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(DropNaTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "dropNa";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String how = config.containsKey("how") ? (String) config.get("how") : "any";

            if (config.containsKey("columns")) {
                List<String> columns = (List<String>) config.get("columns");
                logger.debug("Dropping rows with nulls ({}) in columns: {}", how, columns);
                return input.na().drop(how, columns.toArray(new String[0]));
            } else {
                logger.debug("Dropping rows with nulls ({})", how);
                return input.na().drop(how);
            }

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to drop nulls: " + e.getMessage(), e);
        }
    }
}
