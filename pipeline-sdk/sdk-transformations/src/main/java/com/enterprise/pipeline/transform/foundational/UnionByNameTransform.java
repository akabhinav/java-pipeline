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
 * Union datasets by column names (not position).
 * Handles missing columns gracefully.
 *
 * Config:
 * - datasets: List of dataset names to union
 * - allowMissing: Whether to allow missing columns (default: false)
 *
 * Example:
 * {"type": "unionByName", "config": {"datasets": ["dataset1", "dataset2"], "allowMissing": true}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class UnionByNameTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(UnionByNameTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "unionByName";
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
            boolean allowMissing = config.containsKey("allowMissing")
                    && (Boolean) config.get("allowMissing");

            logger.debug("Union by name with datasets: {} (allowMissing: {})", datasetNames, allowMissing);

            Dataset<Row> result = input;
            for (String datasetName : datasetNames) {
                Dataset<Row> dataset = context.getDataset(datasetName)
                        .orElseThrow(() -> new TransformationException(getName(),
                                "Dataset not found: " + datasetName));
                result = result.unionByName(dataset, allowMissing);
            }

            return result;

        } catch (TransformationException e) {
            throw e;
        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to union datasets by name: " + e.getMessage(), e);
        }
    }
}
