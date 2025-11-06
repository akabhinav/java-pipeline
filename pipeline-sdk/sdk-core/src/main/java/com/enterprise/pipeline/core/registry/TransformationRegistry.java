package com.enterprise.pipeline.core.registry;

import com.enterprise.pipeline.api.Sink;
import com.enterprise.pipeline.api.Source;
import com.enterprise.pipeline.api.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for discovering and managing transformations, sources, and sinks.
 *
 * Uses Java ServiceLoader for plugin discovery (SPI pattern).
 * Thread-safe singleton implementation.
 *
 * Design Principles:
 * - Simple: ServiceLoader handles discovery
 * - Extensible: Just add META-INF/services file
 * - No magic: Standard Java SPI
 *
 * @author Enterprise Data Pipeline Team
 */
public class TransformationRegistry {

    private static final Logger logger = LoggerFactory.getLogger(TransformationRegistry.class);
    private static final TransformationRegistry INSTANCE = new TransformationRegistry();

    private final Map<String, Transformation> transformations = new ConcurrentHashMap<>();
    private final Map<String, Source> sources = new ConcurrentHashMap<>();
    private final Map<String, Sink> sinks = new ConcurrentHashMap<>();

    private TransformationRegistry() {
        // Private constructor for singleton
        discoverPlugins();
    }

    public static TransformationRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Discover all plugins using ServiceLoader.
     */
    private void discoverPlugins() {
        logger.info("Starting plugin discovery...");

        // Discover transformations
        ServiceLoader<Transformation> transformationLoader = ServiceLoader.load(Transformation.class);
        for (Transformation transformation : transformationLoader) {
            registerTransformation(transformation);
        }

        // Discover sources
        ServiceLoader<Source> sourceLoader = ServiceLoader.load(Source.class);
        for (Source source : sourceLoader) {
            registerSource(source);
        }

        // Discover sinks
        ServiceLoader<Sink> sinkLoader = ServiceLoader.load(Sink.class);
        for (Sink sink : sinkLoader) {
            registerSink(sink);
        }

        logger.info("Plugin discovery complete. Found {} transformations, {} sources, {} sinks",
                transformations.size(), sources.size(), sinks.size());
    }

    /**
     * Register a transformation.
     *
     * @param transformation Transformation to register
     */
    public void registerTransformation(Transformation transformation) {
        String name = transformation.getName();
        if (transformations.containsKey(name)) {
            logger.warn("Transformation '{}' already registered, overwriting with {}",
                    name, transformation.getClass().getName());
        }
        transformations.put(name, transformation);
        logger.debug("Registered transformation: {} -> {}", name, transformation.getClass().getName());
    }

    /**
     * Register a source.
     *
     * @param source Source to register
     */
    public void registerSource(Source source) {
        String name = source.getName();
        if (sources.containsKey(name)) {
            logger.warn("Source '{}' already registered, overwriting with {}",
                    name, source.getClass().getName());
        }
        sources.put(name, source);
        logger.debug("Registered source: {} -> {}", name, source.getClass().getName());
    }

    /**
     * Register a sink.
     *
     * @param sink Sink to register
     */
    public void registerSink(Sink sink) {
        String name = sink.getName();
        if (sinks.containsKey(name)) {
            logger.warn("Sink '{}' already registered, overwriting with {}",
                    name, sink.getClass().getName());
        }
        sinks.put(name, sink);
        logger.debug("Registered sink: {} -> {}", name, sink.getClass().getName());
    }

    /**
     * Get a transformation by name.
     *
     * @param name Transformation name
     * @return Optional containing the transformation if found
     */
    public Optional<Transformation> getTransformation(String name) {
        return Optional.ofNullable(transformations.get(name));
    }

    /**
     * Get a source by name.
     *
     * @param name Source name
     * @return Optional containing the source if found
     */
    public Optional<Source> getSource(String name) {
        return Optional.ofNullable(sources.get(name));
    }

    /**
     * Get a sink by name.
     *
     * @param name Sink name
     * @return Optional containing the sink if found
     */
    public Optional<Sink> getSink(String name) {
        return Optional.ofNullable(sinks.get(name));
    }

    /**
     * Get all registered transformation names.
     *
     * @return Set of transformation names
     */
    public Map<String, Transformation> getAllTransformations() {
        return Map.copyOf(transformations);
    }

    /**
     * Get all registered source names.
     *
     * @return Set of source names
     */
    public Map<String, Source> getAllSources() {
        return Map.copyOf(sources);
    }

    /**
     * Get all registered sink names.
     *
     * @return Set of sink names
     */
    public Map<String, Sink> getAllSinks() {
        return Map.copyOf(sinks);
    }
}
