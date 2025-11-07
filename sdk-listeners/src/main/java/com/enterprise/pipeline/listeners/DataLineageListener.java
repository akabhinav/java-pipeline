package com.enterprise.pipeline.listeners;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data Lineage Listener
 *
 * Tracks data lineage across ALL industries for impact analysis and compliance.
 *
 * Lineage Tracking:
 * - Source datasets (files, tables, streams)
 * - Transformations applied (filter, join, aggregate, etc.)
 * - Destination datasets (output locations)
 * - Column-level lineage (which output columns come from which input columns)
 * - Transformation graph (visual representation)
 *
 * Use Cases:
 * - Impact analysis: "If this table changes, what breaks?"
 * - Root cause analysis: "Where did this bad data come from?"
 * - Compliance: "Show all transformations on customer PII data"
 * - Documentation: Auto-generate data flow diagrams
 * - Data governance: Track sensitive data movement
 *
 * @author Enterprise Pipeline Platform
 */
public class DataLineageListener extends SparkListener {

    private static final Logger logger = LoggerFactory.getLogger(DataLineageListener.class);

    private final Map<String, LineageNode> lineageGraph = new ConcurrentHashMap<>();
    private final Map<Integer, List<String>> jobToNodes = new ConcurrentHashMap<>();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        int jobId = jobStart.jobId();
        logger.info("Data lineage tracking started for Job {}", jobId);
        jobToNodes.put(jobId, new ArrayList<>());
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        int jobId = jobEnd.jobId();
        List<String> nodes = jobToNodes.remove(jobId);

        if (nodes != null) {
            logger.info("Data lineage tracked for Job {}: {} nodes", jobId, nodes.size());
            printLineageGraph(jobId);
        }
    }

    /**
     * Track dataset lineage
     * Call this method manually in your pipeline code
     *
     * Example:
     *   Dataset<Row> input = spark.read().csv("input.csv");
     *   lineageListener.trackSource("input_data", "file://input.csv", input);
     *
     *   Dataset<Row> filtered = input.filter("age > 18");
     *   lineageListener.trackTransformation("filtered_data", "filter", List.of("input_data"), filtered);
     *
     *   filtered.write().parquet("output.parquet");
     *   lineageListener.trackSink("output_data", "file://output.parquet", "filtered_data");
     */

    /**
     * Track a source dataset (input)
     */
    public void trackSource(String nodeName, String sourceLocation, Dataset<Row> dataset) {
        LineageNode node = new LineageNode(
            nodeName,
            LineageNodeType.SOURCE,
            sourceLocation,
            null,
            new ArrayList<>(),
            extractSchema(dataset)
        );

        lineageGraph.put(nodeName, node);

        logger.info("Lineage SOURCE tracked: {} -> {}", nodeName, sourceLocation);
        logger.info("  Schema: {}", node.getSchema());
    }

    /**
     * Track a transformation
     */
    public void trackTransformation(String nodeName, String transformationType,
                                   List<String> inputNodes, Dataset<Row> dataset) {
        LineageNode node = new LineageNode(
            nodeName,
            LineageNodeType.TRANSFORMATION,
            transformationType,
            inputNodes,
            new ArrayList<>(),
            extractSchema(dataset)
        );

        lineageGraph.put(nodeName, node);

        // Update parent nodes
        for (String inputNode : inputNodes) {
            LineageNode parent = lineageGraph.get(inputNode);
            if (parent != null) {
                parent.addOutputNode(nodeName);
            }
        }

        logger.info("Lineage TRANSFORMATION tracked: {} <- {} (type: {})",
            nodeName, inputNodes, transformationType);
        logger.info("  Schema: {}", node.getSchema());
    }

    /**
     * Track a sink dataset (output)
     */
    public void trackSink(String nodeName, String sinkLocation, String inputNode) {
        LineageNode node = new LineageNode(
            nodeName,
            LineageNodeType.SINK,
            sinkLocation,
            List.of(inputNode),
            new ArrayList<>(),
            null  // Schema is same as input
        );

        lineageGraph.put(nodeName, node);

        // Update parent node
        LineageNode parent = lineageGraph.get(inputNode);
        if (parent != null) {
            parent.addOutputNode(nodeName);
            node.setSchema(parent.getSchema());  // Inherit schema from input
        }

        logger.info("Lineage SINK tracked: {} -> {}", nodeName, sinkLocation);
    }

    /**
     * Track column-level lineage
     */
    public void trackColumnLineage(String outputColumn, String outputNode,
                                   String inputColumn, String inputNode) {
        logger.info("Column lineage: {}.{} <- {}.{}",
            outputNode, outputColumn, inputNode, inputColumn);

        // TODO: Store column-level lineage for detailed impact analysis
    }

    /**
     * Extract schema from dataset
     */
    private List<String> extractSchema(Dataset<Row> dataset) {
        try {
            return Arrays.asList(dataset.columns());
        } catch (Exception e) {
            logger.warn("Could not extract schema: {}", e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Print lineage graph for a job
     */
    private void printLineageGraph(int jobId) {
        logger.info("===== Data Lineage Graph for Job {} =====", jobId);

        // Print sources
        lineageGraph.values().stream()
            .filter(node -> node.getType() == LineageNodeType.SOURCE)
            .forEach(node -> {
                logger.info("SOURCE: {} ({})", node.getName(), node.getDetails());
                printNodeTree(node, 1);
            });

        logger.info("==========================================");
    }

    /**
     * Print node tree recursively
     */
    private void printNodeTree(LineageNode node, int depth) {
        String indent = "  ".repeat(depth);

        for (String outputNodeName : node.getOutputNodes()) {
            LineageNode outputNode = lineageGraph.get(outputNodeName);
            if (outputNode != null) {
                logger.info("{}{}: {} ({})",
                    indent,
                    outputNode.getType(),
                    outputNode.getName(),
                    outputNode.getDetails());

                printNodeTree(outputNode, depth + 1);
            }
        }
    }

    /**
     * Find upstream dependencies for a node
     */
    public List<LineageNode> getUpstreamDependencies(String nodeName) {
        LineageNode node = lineageGraph.get(nodeName);
        if (node == null) {
            return new ArrayList<>();
        }

        List<LineageNode> upstream = new ArrayList<>();
        collectUpstream(node, upstream, new HashSet<>());
        return upstream;
    }

    /**
     * Find downstream dependencies for a node
     */
    public List<LineageNode> getDownstreamDependencies(String nodeName) {
        LineageNode node = lineageGraph.get(nodeName);
        if (node == null) {
            return new ArrayList<>();
        }

        List<LineageNode> downstream = new ArrayList<>();
        collectDownstream(node, downstream, new HashSet<>());
        return downstream;
    }

    /**
     * Collect upstream dependencies recursively
     */
    private void collectUpstream(LineageNode node, List<LineageNode> result, Set<String> visited) {
        if (visited.contains(node.getName())) {
            return;
        }
        visited.add(node.getName());

        for (String inputName : node.getInputNodes()) {
            LineageNode inputNode = lineageGraph.get(inputName);
            if (inputNode != null) {
                result.add(inputNode);
                collectUpstream(inputNode, result, visited);
            }
        }
    }

    /**
     * Collect downstream dependencies recursively
     */
    private void collectDownstream(LineageNode node, List<LineageNode> result, Set<String> visited) {
        if (visited.contains(node.getName())) {
            return;
        }
        visited.add(node.getName());

        for (String outputName : node.getOutputNodes()) {
            LineageNode outputNode = lineageGraph.get(outputName);
            if (outputNode != null) {
                result.add(outputNode);
                collectDownstream(outputNode, result, visited);
            }
        }
    }

    /**
     * Get full lineage graph
     */
    public Map<String, LineageNode> getLineageGraph() {
        return new ConcurrentHashMap<>(lineageGraph);
    }

    /**
     * Export lineage to DOT format (for Graphviz visualization)
     */
    public String exportToDOT() {
        StringBuilder dot = new StringBuilder();
        dot.append("digraph DataLineage {\n");
        dot.append("  rankdir=LR;\n");
        dot.append("  node [shape=box];\n\n");

        for (LineageNode node : lineageGraph.values()) {
            // Node styling based on type
            String color = switch (node.getType()) {
                case SOURCE -> "lightblue";
                case TRANSFORMATION -> "lightgreen";
                case SINK -> "lightcoral";
            };

            dot.append(String.format("  \"%s\" [style=filled, fillcolor=%s, label=\"%s\\n(%s)\"];\n",
                node.getName(), color, node.getName(), node.getType()));

            // Edges
            for (String outputName : node.getOutputNodes()) {
                dot.append(String.format("  \"%s\" -> \"%s\";\n", node.getName(), outputName));
            }
        }

        dot.append("}\n");
        return dot.toString();
    }

    /**
     * Lineage Node Type
     */
    public enum LineageNodeType {
        SOURCE,          // Input dataset (file, table, stream)
        TRANSFORMATION,  // Intermediate dataset (after transformation)
        SINK             // Output dataset (destination)
    }

    /**
     * Lineage Node POJO
     */
    public static class LineageNode {
        private final String name;
        private final LineageNodeType type;
        private final String details;        // File path, transformation type, etc.
        private final List<String> inputNodes;
        private final List<String> outputNodes;
        private List<String> schema;

        public LineageNode(String name, LineageNodeType type, String details,
                          List<String> inputNodes, List<String> outputNodes,
                          List<String> schema) {
            this.name = name;
            this.type = type;
            this.details = details;
            this.inputNodes = inputNodes != null ? new ArrayList<>(inputNodes) : new ArrayList<>();
            this.outputNodes = outputNodes != null ? new ArrayList<>(outputNodes) : new ArrayList<>();
            this.schema = schema;
        }

        public void addOutputNode(String nodeName) {
            if (!outputNodes.contains(nodeName)) {
                outputNodes.add(nodeName);
            }
        }

        public void setSchema(List<String> schema) {
            this.schema = schema;
        }

        public String getName() { return name; }
        public LineageNodeType getType() { return type; }
        public String getDetails() { return details; }
        public List<String> getInputNodes() { return new ArrayList<>(inputNodes); }
        public List<String> getOutputNodes() { return new ArrayList<>(outputNodes); }
        public List<String> getSchema() { return schema; }

        @Override
        public String toString() {
            return String.format("LineageNode{name=%s, type=%s, inputs=%s, outputs=%s}",
                name, type, inputNodes, outputNodes);
        }
    }
}
