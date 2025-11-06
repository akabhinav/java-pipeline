package com.enterprise.pipeline.transform.foundational;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Cache the dataset in memory for faster access.
 *
 * Config:
 * - storageLevel: Storage level (optional, default: MEMORY_AND_DISK)
 *   Options: MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY
 *
 * Example:
 * {"type": "cache", "config": {"storageLevel": "MEMORY_ONLY"}}
 *
 * @author Enterprise Data Pipeline Team
 */
public class CacheTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CacheTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "cache";
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String storageLevelStr = config.containsKey("storageLevel")
                    ? (String) config.get("storageLevel")
                    : "MEMORY_AND_DISK";

            StorageLevel storageLevel = switch (storageLevelStr.toUpperCase()) {
                case "MEMORY_ONLY" -> StorageLevel.MEMORY_ONLY();
                case "MEMORY_AND_DISK" -> StorageLevel.MEMORY_AND_DISK();
                case "DISK_ONLY" -> StorageLevel.DISK_ONLY();
                default -> StorageLevel.MEMORY_AND_DISK();
            };

            logger.debug("Caching dataset with storage level: {}", storageLevelStr);
            return input.persist(storageLevel);

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to cache dataset: " + e.getMessage(), e);
        }
    }
}
