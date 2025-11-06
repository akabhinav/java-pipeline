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
 * Generic aggregation without grouping.
 * Produces a single row with aggregate values.
 *
 * Config:
 * - aggregations: Map of column -> aggregation function
 *   Functions: sum, count, avg, min, max, first, last, countDistinct
 *
 * Example:
 * {
 *   "type": "aggregate",
 *   "config": {
 *     "aggregations": {
 *       "amount": "sum",
 *       "transaction_id": "count",
 *       "customer_id": "countDistinct"
 *     }
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class AggregateTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(AggregateTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "aggregate";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("aggregations")) {
            throw new ValidationException("'aggregations' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            Map<String, String> aggregations = (Map<String, String>) config.get("aggregations");

            logger.debug("Applying aggregations: {}", aggregations);

            // Build aggregation columns
            List<Column> aggColumns = new ArrayList<>();
            for (Map.Entry<String, String> entry : aggregations.entrySet()) {
                String column = entry.getKey();
                String function = entry.getValue().toLowerCase();

                Column aggCol = switch (function) {
                    case "sum" -> functions.sum(column).alias(column + "_sum");
                    case "count" -> functions.count(column).alias(column + "_count");
                    case "avg", "average" -> functions.avg(column).alias(column + "_avg");
                    case "min" -> functions.min(column).alias(column + "_min");
                    case "max" -> functions.max(column).alias(column + "_max");
                    case "first" -> functions.first(column).alias(column + "_first");
                    case "last" -> functions.last(column).alias(column + "_last");
                    case "countdistinct" -> functions.countDistinct(column).alias(column + "_distinct");
                    default -> throw new IllegalArgumentException("Unsupported aggregation function: " + function);
                };
                aggColumns.add(aggCol);
            }

            return input.agg(aggColumns.get(0), aggColumns.stream().skip(1).toArray(Column[]::new));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to aggregate: " + e.getMessage(), e);
        }
    }
}
