package com.enterprise.pipeline.connector;

import com.enterprise.pipeline.api.Sink;
import com.enterprise.pipeline.api.SinkException;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * File sink connector.
 * Supports CSV, JSON, Parquet, Avro, ORC formats.
 *
 * Config:
 * - path: Output file path or directory
 * - format: File format (csv, json, parquet, avro, orc)
 * - mode: Write mode (optional, default: overwrite)
 *   Options: overwrite, append, ignore, error
 * - options: Map of format-specific options (optional)
 *   For CSV: header, delimiter, etc.
 * - partitionBy: List of columns to partition by (optional)
 *
 * Example:
 * {
 *   "type": "file",
 *   "config": {
 *     "path": "/output/customers.parquet",
 *     "format": "parquet",
 *     "mode": "overwrite",
 *     "partitionBy": ["year", "month"]
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class FileSink implements Sink {

    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "file";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("path")) {
            throw new ValidationException("'path' is required");
        }
        if (!config.containsKey("format")) {
            throw new ValidationException("'format' is required");
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(Dataset<Row> dataset, Map<String, Object> config, TransformationContext context)
            throws SinkException {
        try {
            String path = (String) config.get("path");
            String format = (String) config.get("format");
            String mode = config.containsKey("mode") ? (String) config.get("mode") : "overwrite";

            logger.info("Writing {} file to: {} (mode: {})", format, path, mode);

            DataFrameWriter<Row> writer = dataset.write().format(format).mode(mode);

            // Apply options if provided
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    writer = writer.option(option.getKey(), option.getValue());
                }
            }

            // Partition by columns if specified
            if (config.containsKey("partitionBy")) {
                java.util.List<String> partitionColumns = (java.util.List<String>) config.get("partitionBy");
                writer = writer.partitionBy(partitionColumns.toArray(new String[0]));
            }

            writer.save(path);
            logger.info("Successfully wrote {} rows to {}", dataset.count(), path);

        } catch (Exception e) {
            throw new SinkException(getName(),
                    "Failed to write file: " + e.getMessage(), e);
        }
    }
}
