package com.enterprise.pipeline.connector;

import com.enterprise.pipeline.api.Source;
import com.enterprise.pipeline.api.SourceException;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * AWS S3 source connector.
 * Supports reading from S3 buckets with various file formats.
 *
 * Config:
 * - bucket: S3 bucket name (required)
 * - key: S3 key/prefix (required)
 * - format: File format (csv, json, parquet, avro, orc)
 * - region: AWS region (optional, default: us-east-1)
 * - accessKey: AWS access key (optional, can use IAM role)
 * - secretKey: AWS secret key (optional, can use IAM role)
 * - options: Map of format-specific options (optional)
 *
 * Example:
 * {
 *   "type": "s3",
 *   "config": {
 *     "bucket": "my-data-bucket",
 *     "key": "data/customers.csv",
 *     "format": "csv",
 *     "region": "us-west-2",
 *     "accessKey": "${AWS_ACCESS_KEY}",
 *     "secretKey": "${AWS_SECRET_KEY}",
 *     "options": {
 *       "header": "true",
 *       "inferSchema": "true"
 *     }
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class S3Source implements Source {

    private static final Logger logger = LoggerFactory.getLogger(S3Source.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "s3";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("bucket")) {
            throw new ValidationException("'bucket' is required");
        }
        if (!config.containsKey("key")) {
            throw new ValidationException("'key' is required");
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
            String bucket = (String) config.get("bucket");
            String key = (String) config.get("key");
            String format = (String) config.get("format");
            String region = config.containsKey("region")
                    ? (String) config.get("region")
                    : "us-east-1";

            logger.info("Reading {} from S3: s3a://{}/{}", format, bucket, key);

            SparkSession spark = context.getSparkSession();

            // Configure S3 access
            configureS3Access(spark, config, region);

            // Build S3 path
            String s3Path = String.format("s3a://%s/%s", bucket, key);

            DataFrameReader reader = spark.read().format(format);

            // Apply options if provided
            if (config.containsKey("options")) {
                Map<String, String> options = (Map<String, String>) config.get("options");
                for (Map.Entry<String, String> option : options.entrySet()) {
                    reader = reader.option(option.getKey(), option.getValue());
                }
            }

            return reader.load(s3Path);

        } catch (Exception e) {
            throw new SourceException(getName(),
                    "Failed to read from S3: " + e.getMessage(), e);
        }
    }

    /**
     * Configure S3 access credentials and settings.
     */
    private void configureS3Access(SparkSession spark, Map<String, Object> config, String region) {
        // Set AWS region
        spark.conf().set("spark.hadoop.fs.s3a.endpoint",
                String.format("s3.%s.amazonaws.com", region));

        // Configure credentials if provided
        if (config.containsKey("accessKey") && config.containsKey("secretKey")) {
            String accessKey = (String) config.get("accessKey");
            String secretKey = (String) config.get("secretKey");

            spark.conf().set("spark.hadoop.fs.s3a.access.key", accessKey);
            spark.conf().set("spark.hadoop.fs.s3a.secret.key", secretKey);

            logger.debug("Using provided AWS credentials");
        } else {
            // Use IAM role or instance profile
            spark.conf().set("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider");

            logger.debug("Using IAM role credentials");
        }

        // Enable fast upload
        spark.conf().set("spark.hadoop.fs.s3a.fast.upload", "true");

        // Set connection settings
        spark.conf().set("spark.hadoop.fs.s3a.connection.maximum", "100");
        spark.conf().set("spark.hadoop.fs.s3a.threads.max", "64");
    }
}
