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
 * Return rows that appear in left dataset but not in right dataset (difference).
 *
 * Config:
 * - rightDataset: Name of the right dataset to subtract
 *
 * Example:
 * {"type": "except", "config": {"rightDataset": "cancelled_orders"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class ExceptTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ExceptTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "except";
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

            logger.debug("Subtracting dataset: {}", rightDatasetName);

            Dataset<Row> rightDataset = context.getDataset(rightDatasetName)
                    .orElseThrow(() -> new TransformationException(getName(),
                            "Dataset not found: " + rightDatasetName));

            return input.except(rightDataset);

        } catch (TransformationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to subtract datasets: " + e.getMessage(), e);
        }
    }
}
