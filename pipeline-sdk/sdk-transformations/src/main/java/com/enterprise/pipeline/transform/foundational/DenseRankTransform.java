package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
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
 * Add dense rank within partitions (no gaps in ranking for ties).
 *
 * Config:
 * - partitionBy: List of columns to partition by (optional)
 * - orderBy: List of columns to order by (required)
 * - alias: Column name for dense rank (default: "dense_rank")
 *
 * Example:
 * {
 *   "type": "denseRank",
 *   "config": {
 *     "partitionBy": ["category"],
 *     "orderBy": ["price"],
 *     "alias": "price_rank"
 *   }
 * }
 *
 * Result: 1, 2, 2, 3, 4 (no gap after tie)
 *
 * @author Enterprise Data Pipeline Team
 */
public class DenseRankTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(DenseRankTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "denseRank";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("orderBy")) {
            throw new ValidationException("'orderBy' is required");
        }
        if (!(config.get("orderBy") instanceof List)) {
            throw new ValidationException("'orderBy' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> orderBy = (List<String>) config.get("orderBy");
            String alias = config.containsKey("alias") ? (String) config.get("alias") : "dense_rank";

            logger.debug("Adding dense rank with orderBy: {}, alias: {}", orderBy, alias);

            // Build window spec
            WindowSpec windowSpec = Window.orderBy(
                    orderBy.stream()
                            .map(functions::col)
                            .toArray(org.apache.spark.sql.Column[]::new)
            );

            // Add partition if specified
            if (config.containsKey("partitionBy")) {
                List<String> partitionBy = (List<String>) config.get("partitionBy");
                logger.debug("Partitioning by: {}", partitionBy);
                windowSpec = Window.partitionBy(
                        partitionBy.stream()
                                .map(functions::col)
                                .toArray(org.apache.spark.sql.Column[]::new)
                ).orderBy(
                        orderBy.stream()
                                .map(functions::col)
                                .toArray(org.apache.spark.sql.Column[]::new)
                );
            }

            return input.withColumn(alias, functions.dense_rank().over(windowSpec));

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to add dense rank: " + e.getMessage(), e);
        }
    }
}
