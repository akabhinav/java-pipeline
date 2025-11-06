package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Apply window function (sum, avg, min, max, lag, lead, etc.).
 *
 * Config:
 * - partitionBy: List of columns to partition by (optional)
 * - orderBy: List of columns to order by (optional)
 * - function: Window function to apply (sum, avg, min, max, lag, lead, first, last)
 * - column: Column to apply function to
 * - alias: Column name for result (required)
 *
 * Example - Running total:
 * {
 *   "type": "window",
 *   "config": {
 *     "partitionBy": ["customer_id"],
 *     "orderBy": ["order_date"],
 *     "function": "sum",
 *     "column": "amount",
 *     "alias": "running_total"
 *   }
 * }
 *
 * Example - Lag:
 * {
 *   "type": "window",
 *   "config": {
 *     "partitionBy": ["customer_id"],
 *     "orderBy": ["order_date"],
 *     "function": "lag",
 *     "column": "amount",
 *     "alias": "previous_amount",
 *     "offset": 1
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class WindowTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(WindowTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "window";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("function")) {
            throw new ValidationException("'function' is required");
        }
        if (!config.containsKey("column")) {
            throw new ValidationException("'column' is required");
        }
        if (!config.containsKey("alias")) {
            throw new ValidationException("'alias' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String function = ((String) config.get("function")).toLowerCase();
            String column = (String) config.get("column");
            String alias = (String) config.get("alias");

            logger.debug("Applying window function '{}' on column '{}' as '{}'", function, column, alias);

            // Build window spec
            WindowSpec windowSpec = Window.unboundedPreceding();

            // Add partition if specified
            if (config.containsKey("partitionBy")) {
                List<String> partitionBy = (List<String>) config.get("partitionBy");
                logger.debug("Partitioning by: {}", partitionBy);
                windowSpec = Window.partitionBy(
                        partitionBy.stream()
                                .map(functions::col)
                                .toArray(org.apache.spark.sql.Column[]::new)
                );
            }

            // Add order if specified
            if (config.containsKey("orderBy")) {
                List<String> orderBy = (List<String>) config.get("orderBy");
                logger.debug("Ordering by: {}", orderBy);
                windowSpec = windowSpec.orderBy(
                        orderBy.stream()
                                .map(functions::col)
                                .toArray(org.apache.spark.sql.Column[]::new)
                );
            }

            // Apply window function
            Column windowColumn = switch (function) {
                case "sum" -> functions.sum(column).over(windowSpec);
                case "avg", "average" -> functions.avg(column).over(windowSpec);
                case "min" -> functions.min(column).over(windowSpec);
                case "max" -> functions.max(column).over(windowSpec);
                case "count" -> functions.count(column).over(windowSpec);
                case "first" -> functions.first(column).over(windowSpec);
                case "last" -> functions.last(column).over(windowSpec);
                case "lag" -> {
                    int offset = config.containsKey("offset")
                            ? ((Number) config.get("offset")).intValue()
                            : 1;
                    yield functions.lag(column, offset).over(windowSpec);
                }
                case "lead" -> {
                    int offset = config.containsKey("offset")
                            ? ((Number) config.get("offset")).intValue()
                            : 1;
                    yield functions.lead(column, offset).over(windowSpec);
                }
                default -> throw new IllegalArgumentException("Unsupported window function: " + function);
            };

            return input.withColumn(alias, windowColumn);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to apply window function: " + e.getMessage(), e);
        }
    }
}
