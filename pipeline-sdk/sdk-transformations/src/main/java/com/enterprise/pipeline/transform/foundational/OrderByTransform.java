package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sort the dataset by the specified columns.
 *
 * Config:
 * - columns: List of columns to sort by
 * - ascending: Optional boolean or list of booleans (default: true)
 *
 * Example:
 * {"type": "orderBy", "config": {"columns": ["date", "amount"], "ascending": [true, false]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class OrderByTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(OrderByTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "orderBy";
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

            // Handle ascending parameter
            List<Boolean> ascending = new ArrayList<>();
            if (config.containsKey("ascending")) {
                Object ascendingObj = config.get("ascending");
                if (ascendingObj instanceof Boolean) {
                    // Single boolean for all columns
                    Boolean asc = (Boolean) ascendingObj;
                    for (int i = 0; i < columns.size(); i++) {
                        ascending.add(asc);
                    }
                } else if (ascendingObj instanceof List) {
                    ascending = (List<Boolean>) ascendingObj;
                }
            } else {
                // Default: all ascending
                for (int i = 0; i < columns.size(); i++) {
                    ascending.add(true);
                }
            }

            logger.debug("Ordering by columns: {} (ascending: {})", columns, ascending);

            // Build sort columns
            List<Column> sortCols = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                Column col = functions.col(columns.get(i));
                if (i < ascending.size() && !ascending.get(i)) {
                    col = col.desc();
                } else {
                    col = col.asc();
                }
                sortCols.add(col);
            }

            return input.orderBy(sortCols.toArray(new Column[0]));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to order by: " + e.getMessage(), e);
        }
    }
}
