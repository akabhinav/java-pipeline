package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Pivot transformation - convert row data into columns.
 *
 * Transforms data from long format to wide format by rotating unique values
 * from a column into multiple columns.
 *
 * Config:
 * - groupByColumns: List of columns to group by (required)
 * - pivotColumn: Column to pivot (required)
 * - valueColumn: Column containing values to aggregate (required)
 * - aggregateFunction: Aggregation function (required)
 *   Options: sum, avg, min, max, count, first, last
 * - pivotValues: List of specific values to pivot (optional)
 *   If not specified, uses all distinct values from pivotColumn
 *
 * Example:
 * Input:
 *   customer_id | product | quantity
 *   C1          | Apple   | 10
 *   C1          | Orange  | 5
 *   C2          | Apple   | 8
 *   C2          | Banana  | 3
 *
 * Config:
 * {
 *   "type": "pivot",
 *   "config": {
 *     "groupByColumns": ["customer_id"],
 *     "pivotColumn": "product",
 *     "valueColumn": "quantity",
 *     "aggregateFunction": "sum"
 *   }
 * }
 *
 * Output:
 *   customer_id | Apple | Orange | Banana
 *   C1          | 10    | 5      | null
 *   C2          | 8     | null   | 3
 *
 * @author Enterprise Data Pipeline Team
 */
public class PivotTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(PivotTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "pivot";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("groupByColumns")) {
            throw new ValidationException("'groupByColumns' is required");
        }
        if (!config.containsKey("pivotColumn")) {
            throw new ValidationException("'pivotColumn' is required");
        }
        if (!config.containsKey("valueColumn")) {
            throw new ValidationException("'valueColumn' is required");
        }
        if (!config.containsKey("aggregateFunction")) {
            throw new ValidationException("'aggregateFunction' is required");
        }

        if (!(config.get("groupByColumns") instanceof List)) {
            throw new ValidationException("'groupByColumns' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> groupByColumns = (List<String>) config.get("groupByColumns");
            String pivotColumn = (String) config.get("pivotColumn");
            String valueColumn = (String) config.get("valueColumn");
            String aggregateFunction = (String) config.get("aggregateFunction");

            logger.debug("Pivoting on column '{}' grouped by {}", pivotColumn, groupByColumns);

            // Group by columns
            String[] groupCols = groupByColumns.toArray(new String[0]);
            RelationalGroupedDataset grouped = input.groupBy(groupCols);

            // Pivot with or without specific values
            RelationalGroupedDataset pivoted;
            if (config.containsKey("pivotValues")) {
                List<Object> pivotValues = (List<Object>) config.get("pivotValues");
                pivoted = grouped.pivot(pivotColumn, pivotValues);
                logger.debug("Pivoting with specific values: {}", pivotValues);
            } else {
                pivoted = grouped.pivot(pivotColumn);
                logger.debug("Pivoting with all distinct values from '{}'", pivotColumn);
            }

            // Apply aggregation function
            Dataset<Row> result = switch (aggregateFunction.toLowerCase()) {
                case "sum" -> pivoted.sum(valueColumn);
                case "avg", "average", "mean" -> pivoted.avg(valueColumn);
                case "min" -> pivoted.min(valueColumn);
                case "max" -> pivoted.max(valueColumn);
                case "count" -> pivoted.count();
                case "first" -> pivoted.agg(org.apache.spark.sql.functions.first(valueColumn));
                case "last" -> pivoted.agg(org.apache.spark.sql.functions.last(valueColumn));
                default -> throw new IllegalArgumentException(
                        "Unsupported aggregate function: " + aggregateFunction);
            };

            logger.debug("Applied aggregation function: {}", aggregateFunction);
            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to pivot data: " + e.getMessage(), e);
        }
    }
}
