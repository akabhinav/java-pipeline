package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Unpivot transformation - convert columns into rows.
 *
 * Transforms data from wide format to long format by converting multiple
 * columns into key-value pairs.
 *
 * Config:
 * - idColumns: List of columns to keep as identifiers (required)
 * - unpivotColumns: List of columns to unpivot (required)
 * - variableColumnName: Name for the variable column (default: "variable")
 * - valueColumnName: Name for the value column (default: "value")
 *
 * Example:
 * Input:
 *   customer_id | Apple | Orange | Banana
 *   C1          | 10    | 5      | null
 *   C2          | 8     | null   | 3
 *
 * Config:
 * {
 *   "type": "unpivot",
 *   "config": {
 *     "idColumns": ["customer_id"],
 *     "unpivotColumns": ["Apple", "Orange", "Banana"],
 *     "variableColumnName": "product",
 *     "valueColumnName": "quantity"
 *   }
 * }
 *
 * Output:
 *   customer_id | product | quantity
 *   C1          | Apple   | 10
 *   C1          | Orange  | 5
 *   C1          | Banana  | null
 *   C2          | Apple   | 8
 *   C2          | Orange  | null
 *   C2          | Banana  | 3
 *
 * @author Enterprise Data Pipeline Team
 */
public class UnpivotTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(UnpivotTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "unpivot";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("idColumns")) {
            throw new ValidationException("'idColumns' is required");
        }
        if (!config.containsKey("unpivotColumns")) {
            throw new ValidationException("'unpivotColumns' is required");
        }

        if (!(config.get("idColumns") instanceof List)) {
            throw new ValidationException("'idColumns' must be a list");
        }
        if (!(config.get("unpivotColumns") instanceof List)) {
            throw new ValidationException("'unpivotColumns' must be a list");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            List<String> idColumns = (List<String>) config.get("idColumns");
            List<String> unpivotColumns = (List<String>) config.get("unpivotColumns");
            String variableColumnName = config.containsKey("variableColumnName")
                    ? (String) config.get("variableColumnName")
                    : "variable";
            String valueColumnName = config.containsKey("valueColumnName")
                    ? (String) config.get("valueColumnName")
                    : "value";

            logger.debug("Unpivoting columns: {}", unpivotColumns);

            // Build the unpivot expression using stack function
            // stack(n, 'col1', col1, 'col2', col2, ...) creates n rows
            StringBuilder stackExpr = new StringBuilder();
            stackExpr.append("stack(").append(unpivotColumns.size());

            for (String col : unpivotColumns) {
                stackExpr.append(", '").append(col).append("', `").append(col).append("`");
            }
            stackExpr.append(") as (`").append(variableColumnName).append("`, `").append(valueColumnName).append("`)");

            // Select id columns and unpivoted data
            String[] selectColumns = new String[idColumns.size() + 2];
            for (int i = 0; i < idColumns.size(); i++) {
                selectColumns[i] = "`" + idColumns.get(i) + "`";
            }
            selectColumns[idColumns.size()] = "`" + variableColumnName + "`";
            selectColumns[idColumns.size() + 1] = "`" + valueColumnName + "`";

            Dataset<Row> result = input.selectExpr(
                    idColumns.toArray(new String[0])
            ).selectExpr(
                    String.join(", ", selectColumns[0]),
                    stackExpr.toString()
            );

            // Alternative implementation using Spark SQL's built-in functionality
            // This is more robust and handles the unpivot correctly
            result = unpivotUsingExplode(input, idColumns, unpivotColumns, variableColumnName, valueColumnName);

            logger.debug("Unpivoted to {} rows per record", unpivotColumns.size());
            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to unpivot data: " + e.getMessage(), e);
        }
    }

    /**
     * Unpivot using array and explode approach.
     * More robust than stack function.
     */
    private Dataset<Row> unpivotUsingExplode(
            Dataset<Row> input,
            List<String> idColumns,
            List<String> unpivotColumns,
            String variableColumnName,
            String valueColumnName) {

        // Create an array of structs for each unpivot column
        org.apache.spark.sql.Column[] structCols = new org.apache.spark.sql.Column[unpivotColumns.size()];
        for (int i = 0; i < unpivotColumns.size(); i++) {
            String col = unpivotColumns.get(i);
            structCols[i] = functions.struct(
                    functions.lit(col).as(variableColumnName),
                    functions.col(col).as(valueColumnName)
            );
        }

        // Create array of structs
        Dataset<Row> withArray = input.withColumn("_unpivot_array",
                functions.array(structCols));

        // Explode the array
        Dataset<Row> exploded = withArray.withColumn("_unpivot_struct",
                functions.explode(functions.col("_unpivot_array")));

        // Extract fields from struct
        Dataset<Row> result = exploded;
        for (String idCol : idColumns) {
            result = result.withColumn(idCol, functions.col(idCol));
        }
        result = result.withColumn(variableColumnName,
                        functions.col("_unpivot_struct").getField(variableColumnName))
                .withColumn(valueColumnName,
                        functions.col("_unpivot_struct").getField(valueColumnName));

        // Select only the required columns - convert to Column array for varargs
        String[] finalColumns = new String[idColumns.size() + 2];
        for (int i = 0; i < idColumns.size(); i++) {
            finalColumns[i] = idColumns.get(i);
        }
        finalColumns[idColumns.size()] = variableColumnName;
        finalColumns[idColumns.size() + 1] = valueColumnName;

        org.apache.spark.sql.Column[] columnArray = java.util.Arrays.stream(finalColumns)
                .map(functions::col)
                .toArray(org.apache.spark.sql.Column[]::new);
        return result.select(columnArray);
    }
}
