package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Flatten nested structures in a DataFrame.
 *
 * Converts nested struct columns into flat columns with dot notation.
 * For example, a column "address.city" becomes "address_city".
 *
 * Config:
 * - separator: Separator to use for flattened column names (default: "_")
 * - maxDepth: Maximum depth to flatten (optional, default: unlimited)
 * - columns: List of specific columns to flatten (optional, default: all struct columns)
 *
 * Example:
 * Input Schema:
 *   customer_id: string
 *   address: struct
 *     - street: string
 *     - city: string
 *     - zipcode: string
 *   contact: struct
 *     - phone: string
 *     - email: string
 *
 * Config:
 * {
 *   "type": "flatten",
 *   "config": {
 *     "separator": "_"
 *   }
 * }
 *
 * Output Schema:
 *   customer_id: string
 *   address_street: string
 *   address_city: string
 *   address_zipcode: string
 *   contact_phone: string
 *   contact_email: string
 *
 * @author Enterprise Data Pipeline Team
 */
public class FlattenTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(FlattenTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "flatten";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        // No required config parameters
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String separator = config.containsKey("separator")
                    ? (String) config.get("separator")
                    : "_";
            Integer maxDepth = config.containsKey("maxDepth")
                    ? ((Number) config.get("maxDepth")).intValue()
                    : Integer.MAX_VALUE;
            List<String> specificColumns = config.containsKey("columns")
                    ? (List<String>) config.get("columns")
                    : null;

            logger.debug("Flattening nested structures with separator '{}' and maxDepth {}",
                    separator, maxDepth);

            Dataset<Row> result = input;
            StructType schema = input.schema();

            // Build flattened select expressions
            List<String> selectExprs = new ArrayList<>();
            flattenSchema(schema, "", selectExprs, separator, maxDepth, 0, specificColumns);

            // Apply the flattened selection
            if (!selectExprs.isEmpty()) {
                result = input.selectExpr(selectExprs.toArray(new String[0]));
                logger.debug("Flattened {} columns", selectExprs.size());
            }

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to flatten data: " + e.getMessage(), e);
        }
    }

    /**
     * Recursively flatten the schema and build select expressions.
     */
    private void flattenSchema(
            StructType schema,
            String prefix,
            List<String> selectExprs,
            String separator,
            int maxDepth,
            int currentDepth,
            List<String> specificColumns) {

        if (currentDepth >= maxDepth) {
            // Reached max depth, add remaining columns as-is
            for (StructField field : schema.fields()) {
                String fullName = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
                String alias = prefix.isEmpty() ? field.name() : prefix + separator + field.name();
                selectExprs.add(String.format("`%s` as `%s`", fullName, alias));
            }
            return;
        }

        for (StructField field : schema.fields()) {
            String fieldName = field.name();
            String fullName = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
            String alias = prefix.isEmpty() ? fieldName : prefix + separator + fieldName;
            DataType dataType = field.dataType();

            // Check if this column should be processed
            if (specificColumns != null && !specificColumns.isEmpty()) {
                boolean shouldProcess = false;
                String rootColumn = prefix.isEmpty() ? fieldName : prefix.split("\\.")[0];
                if (specificColumns.contains(rootColumn)) {
                    shouldProcess = true;
                }
                if (!shouldProcess) {
                    // Skip this column
                    if (prefix.isEmpty()) {
                        selectExprs.add(String.format("`%s`", fieldName));
                    }
                    continue;
                }
            }

            if (dataType instanceof StructType) {
                // Nested struct - recurse
                StructType structType = (StructType) dataType;
                flattenSchema(structType, fullName, selectExprs, separator,
                        maxDepth, currentDepth + 1, specificColumns);
            } else if (dataType.typeName().startsWith("array")) {
                // Array type - keep as-is (could explode if needed)
                selectExprs.add(String.format("`%s` as `%s`", fullName, alias));
            } else if (dataType.typeName().startsWith("map")) {
                // Map type - keep as-is (could explode if needed)
                selectExprs.add(String.format("`%s` as `%s`", fullName, alias));
            } else {
                // Primitive type - add to select
                selectExprs.add(String.format("`%s` as `%s`", fullName, alias));
            }
        }
    }
}
