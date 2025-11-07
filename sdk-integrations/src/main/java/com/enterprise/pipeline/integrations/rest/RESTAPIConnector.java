package com.enterprise.pipeline.integrations.rest;

import com.enterprise.pipeline.integrations.common.ConnectionConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * REST API connector for reading and writing data via HTTP/HTTPS.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Read from REST API
 * ConnectionConfig config = ConnectionConfig.builder()
 *     .url("https://api.example.com/users")
 *     .set("method", "GET")
 *     .set("headers", Map.of("Authorization", "Bearer token"))
 *     .build();
 *
 * Dataset<Row> data = RESTAPIConnector.read(spark, config);
 *
 * // Write to REST API
 * RESTAPIConnector.write(data, config, "POST");
 * }</pre>
 */
public class RESTAPIConnector implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(RESTAPIConnector.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Read data from REST API.
     */
    public static Dataset<Row> read(SparkSession spark, ConnectionConfig config) {
        String url = config.getString("url");
        String method = config.getString("method", "GET");

        logger.info("Reading from REST API: {} {}", method, url);

        try {
            String jsonResponse = executeRequest(url, method, config, null);

            // Parse JSON response and convert to Dataset
            Dataset<Row> dataset = spark.read().json(
                spark.sparkContext().parallelize(Collections.singletonList(jsonResponse), 1),
                spark.sparkContext().defaultParallelism()
            );

            logger.info("Successfully read {} rows from REST API", dataset.count());
            return dataset;

        } catch (Exception e) {
            logger.error("Error reading from REST API", e);
            throw new RuntimeException("Failed to read from REST API: " + url, e);
        }
    }

    /**
     * Write data to REST API.
     */
    public static void write(Dataset<Row> dataset, ConnectionConfig config, String method) {
        String url = config.getString("url");

        logger.info("Writing to REST API: {} {}", method, url);

        // Convert dataset to JSON and send
        List<String> jsonRecords = dataset.toJSON().collectAsList();

        int batchSize = config.getInt("batchSize", 100);
        int successCount = 0;
        int failureCount = 0;

        for (int i = 0; i < jsonRecords.size(); i += batchSize) {
            List<String> batch = jsonRecords.subList(i, Math.min(i + batchSize, jsonRecords.size()));

            try {
                String payload = "[" + String.join(",", batch) + "]";
                executeRequest(url, method, config, payload);
                successCount += batch.size();

            } catch (Exception e) {
                logger.error("Error writing batch to REST API", e);
                failureCount += batch.size();
            }
        }

        logger.info("Wrote {} records to REST API ({} succeeded, {} failed)",
            jsonRecords.size(), successCount, failureCount);
    }

    /**
     * Execute HTTP request.
     */
    private static String executeRequest(String url, String method, ConnectionConfig config,
                                        String payload) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

            HttpUriRequestBase request = createRequest(url, method);

            // Add headers
            if (config.containsKey("headers")) {
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) config.get("headers");
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    request.addHeader(header.getKey(), header.getValue());
                }
            }

            // Add payload for POST/PUT/PATCH
            if (payload != null && (request instanceof HttpPost || request instanceof HttpPut || request instanceof HttpPatch)) {
                StringEntity entity = new StringEntity(payload);
                entity.setContentType("application/json");
                if (request instanceof HttpPost) {
                    ((HttpPost) request).setEntity(entity);
                } else if (request instanceof HttpPut) {
                    ((HttpPut) request).setEntity(entity);
                } else {
                    ((HttpPatch) request).setEntity(entity);
                }
            }

            // Execute request
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                int statusCode = response.getCode();

                if (statusCode >= 200 && statusCode < 300) {
                    return EntityUtils.toString(response.getEntity());
                } else {
                    String errorBody = EntityUtils.toString(response.getEntity());
                    throw new RuntimeException("HTTP " + statusCode + ": " + errorBody);
                }
            }
        }
    }

    /**
     * Create HTTP request based on method.
     */
    private static HttpUriRequestBase createRequest(String url, String method) {
        return switch (method.toUpperCase()) {
            case "GET" -> new HttpGet(url);
            case "POST" -> new HttpPost(url);
            case "PUT" -> new HttpPut(url);
            case "PATCH" -> new HttpPatch(url);
            case "DELETE" -> new HttpDelete(url);
            default -> throw new IllegalArgumentException("Unsupported HTTP method: " + method);
        };
    }

    /**
     * Read from paginated API.
     */
    public static Dataset<Row> readPaginated(SparkSession spark, ConnectionConfig config) {
        String baseUrl = config.getString("url");
        String pageParam = config.getString("pageParam", "page");
        int maxPages = config.getInt("maxPages", 100);

        logger.info("Reading paginated data from REST API: {}", baseUrl);

        List<String> allRecords = new ArrayList<>();
        int page = 1;

        while (page <= maxPages) {
            try {
                String url = baseUrl + (baseUrl.contains("?") ? "&" : "?") + pageParam + "=" + page;
                ConnectionConfig pageConfig = ConnectionConfig.builder()
                    .url(url)
                    .set("method", "GET")
                    .set("headers", config.get("headers"))
                    .build();

                String response = executeRequest(url, "GET", pageConfig, null);

                if (response == null || response.trim().isEmpty() || response.equals("[]")) {
                    break; // No more data
                }

                allRecords.add(response);
                page++;

            } catch (Exception e) {
                logger.warn("Error reading page {}: {}", page, e.getMessage());
                break;
            }
        }

        logger.info("Read {} pages from paginated API", page - 1);

        return spark.read().json(
            spark.sparkContext().parallelize(allRecords, allRecords.size()),
            spark.sparkContext().defaultParallelism()
        );
    }
}
