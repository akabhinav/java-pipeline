package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;

/**
 * Left anti join with another dataset.
 * Returns only rows from left dataset that have NO matches in right dataset.
 *
 * Config:
 * - rightDataset: Name of the right dataset to join with
 * - joinColumns: List of columns to join on
 *
 * Example:
 * {
 *   "type": "leftAntiJoin",
 *   "config": {
 *     "rightDataset": "blacklisted_customers",
 *     "joinColumns": ["customer_id"]
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class LeftAntiJoinTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(LeftAntiJoinTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "leftAntiJoin";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("rightDataset")) {
            throw new ValidationException("'rightDataset' is required");
        }
        if (!config.containsKey("joinColumns")) {
            throw new ValidationException("'joinColumns' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String rightDatasetName = (String) config.get("rightDataset");
            List<String> joinColumns = (List<String>) config.get("joinColumns");

            logger.debug("Left anti joining with '{}' on columns: {}", rightDatasetName, joinColumns);

            Dataset<Row> rightDataset = context.getDataset(rightDatasetName)
                    .orElseThrow(() -> new TransformationException(getName(),
                            "Right dataset not found: " + rightDatasetName));

            return input.join(rightDataset,
                    JavaConverters.asScalaBuffer(joinColumns).toSeq(),
                    "left_anti");

        } catch (TransformationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to perform left anti join: " + e.getMessage(), e);
        }
    }
}
