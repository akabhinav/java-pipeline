package com.enterprise.pipeline.connector;

import com.enterprise.pipeline.api.Source;
import com.enterprise.pipeline.api.SourceException;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * File source connector.
 * Supports CSV, JSON, Parquet, Avro, ORC formats.
 *
 * Config:
 * - path: File path or directory
 * - format: File format (csv, json, parquet, avro, orc)
 * - options: Map of format-specific options (optional)
 *   For CSV: header, delimiter, inferSchema, etc.
 *
 * Example:
 * {
 *   "type": "file",
 *   "config": {
 *     "path": "/data/customers.csv",
 *     "format": "csv",
 *     "options": {
 *       "header": "true",
 *       "inferSchema": "true"
 *     }
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class FileSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(FileSource.class);
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
    public Dataset<Row> read(Map<String, Object> config, TransformationContext context)
            throws SourceException {
        try {
            String path = (String) config.get("path");
            String format = (String) config.get("format");

            logger.info("Reading {} file from: {}", format, path);

            DataFrameReader reader = context.getSparkSession().read().format(format);

            // Apply options if provided
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    reader = reader.option(option.getKey(), option.getValue());
                }
            }

            return reader.load(path);

        } catch (Exception e) {
            throw new SourceException(getName(),
                    "Failed to read file: " + e.getMessage(), e);
        }
    }
}
