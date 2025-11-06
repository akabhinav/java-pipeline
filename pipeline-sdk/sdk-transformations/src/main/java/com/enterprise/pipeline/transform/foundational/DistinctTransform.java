package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Remove duplicate rows from the dataset.
 *
 * Config: None required
 *
 * Example:
 * {"type": "distinct", "config": {}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class DistinctTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(DistinctTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "distinct";
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            logger.debug("Removing duplicate rows");
            return input.distinct();

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to remove duplicates: " + e.getMessage(), e);
        }
    }
}
