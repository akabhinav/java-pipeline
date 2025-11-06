package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Union multiple datasets.
 *
 * Config:
 * - datasets: List of dataset names to union
 *
 * Example:
 * {"type": "union", "config": {"datasets": ["dataset1", "dataset2"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class UnionTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(UnionTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "union";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("datasets")) {
            throw new ValidationException("'datasets' is required");
        }
        if (!(config.get("datasets") instanceof List)) {
            throw new ValidationException("'datasets' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> datasetNames = (List<String>) config.get("datasets");
            logger.debug("Unioning with datasets: {}", datasetNames);

            Dataset<Row> result = input;
            for (String datasetName : datasetNames) {
                Dataset<Row> dataset = context.getDataset(datasetName)
                        .orElseThrow(() -> new TransformationException(getName(),
                                "Dataset not found: " + datasetName));
                result = result.union(dataset);
            }

            return result;

        } catch (TransformationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to union datasets: " + e.getMessage(), e);
        }
    }
}
