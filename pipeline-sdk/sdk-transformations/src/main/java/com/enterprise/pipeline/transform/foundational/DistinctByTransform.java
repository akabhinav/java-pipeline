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
 * Remove duplicates based on specific columns.
 *
 * Config:
 * - columns: List of columns to consider for distinctness
 *
 * Example:
 * {"type": "distinctBy", "config": {"columns": ["customer_id", "order_date"]}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class DistinctByTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(DistinctByTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "distinctBy";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("columns")) {
            throw new ValidationException("'columns' is required");
        }
        if (!(config.get("columns") instanceof List)) {
            throw new ValidationException("'columns' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> columns = (List<String>) config.get("columns");
            logger.debug("Removing duplicates based on columns: {}", columns);

            // Convert list to varargs (Scala Seq)
            scala.collection.Seq<String> colSeq = scala.collection.JavaConverters
                    .asScalaBuffer(columns).toSeq();
            return input.dropDuplicates(colSeq);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to remove duplicates: " + e.getMessage(), e);
        }
    }
}
