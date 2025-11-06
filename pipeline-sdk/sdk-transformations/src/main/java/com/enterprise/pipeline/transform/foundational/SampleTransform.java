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
 * Random sample of rows from the dataset.
 *
 * Config:
 * - fraction: Fraction of rows to sample (0.0 to 1.0)
 * - withReplacement: Whether to sample with replacement (optional, default: false)
 * - seed: Random seed (optional)
 *
 * Example:
 * {"type": "sample", "config": {"fraction": 0.1, "withReplacement": false}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class SampleTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(SampleTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "sample";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("fraction")) {
            throw new ValidationException("'fraction' is required");
        }
        Object fraction = config.get("fraction");
        if (!(fraction instanceof Number)) {
            throw new ValidationException("'fraction' must be a number");
        }
        double fractionValue = ((Number) fraction).doubleValue();
        if (fractionValue < 0.0 || fractionValue > 1.0) {
            throw new ValidationException("'fraction' must be between 0.0 and 1.0");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            double fraction = ((Number) config.get("fraction")).doubleValue();
            boolean withReplacement = config.containsKey("withReplacement")
                    && (Boolean) config.get("withReplacement");

            logger.debug("Sampling {}% of rows (withReplacement: {})", fraction * 100, withReplacement);

            if (config.containsKey("seed")) {
                long seed = ((Number) config.get("seed")).longValue();
                return input.sample(withReplacement, fraction, seed);
            } else {
                return input.sample(withReplacement, fraction);
            }

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to sample rows: " + e.getMessage(), e);
        }
    }
}
