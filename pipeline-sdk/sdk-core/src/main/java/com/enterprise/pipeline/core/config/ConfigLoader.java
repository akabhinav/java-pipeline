package com.enterprise.pipeline.core.config;

import com.enterprise.pipeline.api.PipelineException;
import com.enterprise.pipeline.api.model.PipelineConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loader for pipeline configurations from JSON or YAML files.
 *
 * Design Principles:
 * - Simple: Uses Jackson for parsing
 * - Flexible: Supports JSON and YAML
 * - Clear error messages: Helps debugging
 *
 * @author Enterprise Data Pipeline Team
 */
public class ConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    private final ObjectMapper jsonMapper;
    private final ObjectMapper yamlMapper;

    public ConfigLoader() {
        this.jsonMapper = new ObjectMapper();
        this.yamlMapper = new ObjectMapper(new YAMLFactory());
    }

    /**
     * Load pipeline config from a file.
     * Automatically detects JSON or YAML based on extension.
     *
     * @param filePath Path to config file
     * @return Parsed pipeline configuration
     * @throws PipelineException if loading fails
     */
    public PipelineConfig loadFromFile(String filePath) throws PipelineException {
        logger.info("Loading pipeline config from: {}", filePath);

        try {
            Path path = Path.of(filePath);
            if (!Files.exists(path)) {
                throw new PipelineException("Config file not found: " + filePath);
            }

            String content = Files.readString(path);
            return loadFromString(content, filePath);

        } catch (IOException e) {
            throw new PipelineException("Failed to read config file: " + filePath, e);
        }
    }

    /**
     * Load pipeline config from a file object.
     *
     * @param file Config file
     * @return Parsed pipeline configuration
     * @throws PipelineException if loading fails
     */
    public PipelineConfig loadFromFile(File file) throws PipelineException {
        return loadFromFile(file.getAbsolutePath());
    }

    /**
     * Load pipeline config from an input stream.
     * Assumes JSON format.
     *
     * @param inputStream Input stream containing config
     * @return Parsed pipeline configuration
     * @throws PipelineException if loading fails
     */
    public PipelineConfig loadFromStream(InputStream inputStream) throws PipelineException {
        logger.info("Loading pipeline config from input stream");

        try {
            return jsonMapper.readValue(inputStream, PipelineConfig.class);
        } catch (IOException e) {
            throw new PipelineException("Failed to parse config from stream: " + e.getMessage(), e);
        }
    }

    /**
     * Load pipeline config from a string.
     *
     * @param content Config content
     * @param source Source description (for error messages)
     * @return Parsed pipeline configuration
     * @throws PipelineException if parsing fails
     */
    public PipelineConfig loadFromString(String content, String source) throws PipelineException {
        logger.debug("Parsing config from: {}", source);

        try {
            // Try JSON first
            if (content.trim().startsWith("{")) {
                return jsonMapper.readValue(content, PipelineConfig.class);
            }
            // Otherwise try YAML
            else {
                return yamlMapper.readValue(content, PipelineConfig.class);
            }
        } catch (IOException e) {
            throw new PipelineException("Failed to parse config from " + source + ": " + e.getMessage(), e);
        }
    }

    /**
     * Load pipeline config from JSON string.
     *
     * @param json JSON string
     * @return Parsed pipeline configuration
     * @throws PipelineException if parsing fails
     */
    public PipelineConfig loadFromJson(String json) throws PipelineException {
        try {
            return jsonMapper.readValue(json, PipelineConfig.class);
        } catch (IOException e) {
            throw new PipelineException("Failed to parse JSON config: " + e.getMessage(), e);
        }
    }

    /**
     * Load pipeline config from YAML string.
     *
     * @param yaml YAML string
     * @return Parsed pipeline configuration
     * @throws PipelineException if parsing fails
     */
    public PipelineConfig loadFromYaml(String yaml) throws PipelineException {
        try {
            return yamlMapper.readValue(yaml, PipelineConfig.class);
        } catch (IOException e) {
            throw new PipelineException("Failed to parse YAML config: " + e.getMessage(), e);
        }
    }

    /**
     * Save pipeline config to a file.
     *
     * @param config Pipeline configuration
     * @param filePath Output file path
     * @throws PipelineException if saving fails
     */
    public void saveToFile(PipelineConfig config, String filePath) throws PipelineException {
        logger.info("Saving pipeline config to: {}", filePath);

        try {
            ObjectMapper mapper = filePath.endsWith(".yaml") || filePath.endsWith(".yml")
                    ? yamlMapper : jsonMapper;

            mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(new File(filePath), config);

        } catch (IOException e) {
            throw new PipelineException("Failed to write config file: " + filePath, e);
        }
    }
}
