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
 * Cross join (Cartesian product) with another dataset.
 * WARNING: This can produce very large datasets.
 *
 * Config:
 * - rightDataset: Name of the right dataset to join with
 *
 * Example:
 * {
 *   "type": "crossJoin",
 *   "config": {
 *     "rightDataset": "date_dimension"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class CrossJoinTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CrossJoinTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "crossJoin";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("rightDataset")) {
            throw new ValidationException("'rightDataset' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String rightDatasetName = (String) config.get("rightDataset");

            logger.warn("Performing cross join with '{}' - this may produce a large dataset", rightDatasetName);

            Dataset<Row> rightDataset = context.getDataset(rightDatasetName)
                    .orElseThrow(() -> new TransformationException(getName(),
                            "Right dataset not found: " + rightDatasetName));

            return input.crossJoin(rightDataset);

        } catch (TransformationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to perform cross join: " + e.getMessage(), e);
        }
    }
}
