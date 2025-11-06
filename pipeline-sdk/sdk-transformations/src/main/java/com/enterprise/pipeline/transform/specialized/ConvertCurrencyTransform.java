package com.enterprise.pipeline.transform.specialized;

import com.enterprise.pipeline.api.Transformation;
import com.enterprise.pipeline.api.TransformationContext;
import com.enterprise.pipeline.api.TransformationException;
import com.enterprise.pipeline.api.ValidationException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Convert currency amounts from one currency to another using exchange rates.
 *
 * Config:
 * - amountColumn: Column containing the amount to convert (required)
 * - fromCurrencyColumn: Column containing source currency code (required if not using fixed fromCurrency)
 * - fromCurrency: Fixed source currency code (optional, overrides fromCurrencyColumn)
 * - toCurrency: Target currency code (required)
 * - exchangeRateColumn: Column containing exchange rate (optional, if available)
 * - exchangeRate: Fixed exchange rate (optional, if not using dynamic rates)
 * - resultColumn: Output column name (default: "converted_amount")
 *
 * Exchange Rate Priority:
 * 1. exchangeRateColumn (if provided)
 * 2. exchangeRate (if provided)
 * 3. Built-in rate lookup (if supported)
 *
 * Example:
 * {
 *   "type": "convertCurrency",
 *   "config": {
 *     "amountColumn": "usd_amount",
 *     "fromCurrency": "USD",
 *     "toCurrency": "EUR",
 *     "exchangeRate": 0.92,
 *     "resultColumn": "eur_amount"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class ConvertCurrencyTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ConvertCurrencyTransform.class);
    private static final long serialVersionUID = 1L;

    // Built-in exchange rates (example rates, should be updated from external source)
    private static final Map<String, Map<String, Double>> EXCHANGE_RATES = Map.of(
            "USD", Map.of("EUR", 0.92, "GBP", 0.79, "JPY", 149.50, "CHF", 0.88, "CAD", 1.36, "AUD", 1.52, "INR", 83.12),
            "EUR", Map.of("USD", 1.09, "GBP", 0.86, "JPY", 162.50, "CHF", 0.96, "CAD", 1.48, "AUD", 1.65, "INR", 90.35),
            "GBP", Map.of("USD", 1.27, "EUR", 1.16, "JPY", 189.20, "CHF", 1.12, "CAD", 1.72, "AUD", 1.92, "INR", 105.18),
            "JPY", Map.of("USD", 0.0067, "EUR", 0.0062, "GBP", 0.0053, "CHF", 0.0059, "CAD", 0.0091, "AUD", 0.0102, "INR", 0.556),
            "INR", Map.of("USD", 0.012, "EUR", 0.011, "GBP", 0.0095, "JPY", 1.80, "CHF", 0.011, "CAD", 0.016, "AUD", 0.018)
    );

    @Override
    public String getName() {
        return "convertCurrency";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("amountColumn")) {
            throw new ValidationException("'amountColumn' is required");
        }
        if (!config.containsKey("toCurrency")) {
            throw new ValidationException("'toCurrency' is required");
        }

        // Must have either fromCurrency or fromCurrencyColumn
        if (!config.containsKey("fromCurrency") && !config.containsKey("fromCurrencyColumn")) {
            throw new ValidationException("Either 'fromCurrency' or 'fromCurrencyColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String amountColumn = (String) config.get("amountColumn");
            String toCurrency = (String) config.get("toCurrency");
            String resultColumn = config.containsKey("resultColumn")
                    ? (String) config.get("resultColumn")
                    : "converted_amount";

            logger.debug("Converting currency to: {}", toCurrency);

            Dataset<Row> result = input;

            // Case 1: Exchange rate column provided
            if (config.containsKey("exchangeRateColumn")) {
                String exchangeRateColumn = (String) config.get("exchangeRateColumn");
                result = result.withColumn(resultColumn,
                        functions.col(amountColumn).multiply(functions.col(exchangeRateColumn)));
                logger.debug("Using exchange rate from column: {}", exchangeRateColumn);
            }
            // Case 2: Fixed exchange rate provided
            else if (config.containsKey("exchangeRate")) {
                double exchangeRate = ((Number) config.get("exchangeRate")).doubleValue();
                result = result.withColumn(resultColumn,
                        functions.col(amountColumn).multiply(functions.lit(exchangeRate)));
                logger.debug("Using fixed exchange rate: {}", exchangeRate);
            }
            // Case 3: Fixed source currency
            else if (config.containsKey("fromCurrency")) {
                String fromCurrency = (String) config.get("fromCurrency");
                Double rate = lookupExchangeRate(fromCurrency, toCurrency);
                result = result.withColumn(resultColumn,
                        functions.col(amountColumn).multiply(functions.lit(rate)));
                logger.debug("Using built-in exchange rate: {} -> {} = {}", fromCurrency, toCurrency, rate);
            }
            // Case 4: Dynamic source currency column
            else if (config.containsKey("fromCurrencyColumn")) {
                String fromCurrencyColumn = (String) config.get("fromCurrencyColumn");
                // Use CASE WHEN for different currency pairs
                result = result.withColumn(resultColumn,
                        buildCurrencyConversionExpression(fromCurrencyColumn, toCurrency, amountColumn));
                logger.debug("Using dynamic currency conversion from column: {}", fromCurrencyColumn);
            }

            // Add currency code column if requested
            result = result.withColumn(resultColumn + "_currency", functions.lit(toCurrency));

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to convert currency: " + e.getMessage(), e);
        }
    }

    /**
     * Lookup exchange rate from built-in rates.
     */
    private Double lookupExchangeRate(String fromCurrency, String toCurrency) {
        if (fromCurrency.equals(toCurrency)) {
            return 1.0;
        }

        if (EXCHANGE_RATES.containsKey(fromCurrency)) {
            Map<String, Double> rates = EXCHANGE_RATES.get(fromCurrency);
            if (rates.containsKey(toCurrency)) {
                return rates.get(toCurrency);
            }
        }

        throw new IllegalArgumentException(
                String.format("No exchange rate found for %s -> %s", fromCurrency, toCurrency));
    }

    /**
     * Build SQL CASE WHEN expression for dynamic currency conversion.
     */
    private org.apache.spark.sql.Column buildCurrencyConversionExpression(
            String fromCurrencyColumn, String toCurrency, String amountColumn) {

        org.apache.spark.sql.Column result = null;

        for (Map.Entry<String, Map<String, Double>> fromEntry : EXCHANGE_RATES.entrySet()) {
            String fromCurrency = fromEntry.getKey();
            Map<String, Double> toRates = fromEntry.getValue();

            Double rate;
            if (fromCurrency.equals(toCurrency)) {
                rate = 1.0;
            } else if (toRates.containsKey(toCurrency)) {
                rate = toRates.get(toCurrency);
            } else {
                continue;
            }

            org.apache.spark.sql.Column condition = functions.col(fromCurrencyColumn).equalTo(fromCurrency);
            org.apache.spark.sql.Column value = functions.col(amountColumn).multiply(functions.lit(rate));

            if (result == null) {
                result = functions.when(condition, value);
            } else {
                result = result.when(condition, value);
            }
        }

        // Default: return original amount if currency not recognized
        if (result != null) {
            result = result.otherwise(functions.col(amountColumn));
        } else {
            result = functions.col(amountColumn);
        }

        return result;
    }
}
