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
 * Categorize banking transactions based on merchant, description, and amount.
 *
 * Categories:
 * - GROCERIES: Supermarkets, food stores
 * - DINING: Restaurants, cafes, food delivery
 * - TRANSPORTATION: Gas, parking, public transit, ride-sharing
 * - UTILITIES: Electric, water, gas, internet, phone
 * - HEALTHCARE: Medical, pharmacy, insurance
 * - ENTERTAINMENT: Movies, streaming, games, concerts
 * - SHOPPING: Retail, online shopping, clothing
 * - TRAVEL: Hotels, airlines, car rentals
 * - EDUCATION: Tuition, books, courses
 * - FINANCE: Loan payments, investments, insurance
 * - INCOME: Salary, refunds, reimbursements
 * - TRANSFER: Account transfers, peer-to-peer
 * - ATM: Cash withdrawals
 * - OTHER: Uncategorized
 *
 * Config:
 * - descriptionColumn: Column containing transaction description (required)
 * - merchantColumn: Column containing merchant name (optional)
 * - amountColumn: Column containing transaction amount (optional)
 * - categoryColumn: Output column for category (default: "transaction_category")
 * - subcategoryColumn: Output column for subcategory (default: "transaction_subcategory")
 *
 * Example:
 * {
 *   "type": "categorizeTransaction",
 *   "config": {
 *     "descriptionColumn": "description",
 *     "merchantColumn": "merchant_name",
 *     "amountColumn": "amount",
 *     "categoryColumn": "category"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class CategorizeTransactionTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(CategorizeTransactionTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "categorizeTransaction";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("descriptionColumn")) {
            throw new ValidationException("'descriptionColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String descriptionColumn = (String) config.get("descriptionColumn");
            String merchantColumn = config.containsKey("merchantColumn")
                    ? (String) config.get("merchantColumn")
                    : null;
            String amountColumn = config.containsKey("amountColumn")
                    ? (String) config.get("amountColumn")
                    : null;
            String categoryColumn = config.containsKey("categoryColumn")
                    ? (String) config.get("categoryColumn")
                    : "transaction_category";
            String subcategoryColumn = config.containsKey("subcategoryColumn")
                    ? (String) config.get("subcategoryColumn")
                    : "transaction_subcategory";

            logger.debug("Categorizing transactions");

            Dataset<Row> result = input;

            // Create lowercase description for matching
            String searchColumn = descriptionColumn;
            if (merchantColumn != null) {
                result = result.withColumn("_search_text",
                        functions.lower(functions.concat(
                                functions.col(descriptionColumn),
                                functions.lit(" "),
                                functions.col(merchantColumn)
                        ))
                );
                searchColumn = "_search_text";
            } else {
                result = result.withColumn("_search_text",
                        functions.lower(functions.col(descriptionColumn))
                );
                searchColumn = "_search_text";
            }

            // Build category assignment using CASE WHEN logic
            org.apache.spark.sql.Column categoryExpr = buildCategoryExpression(searchColumn, amountColumn);
            result = result.withColumn(categoryColumn, categoryExpr);

            // Build subcategory
            org.apache.spark.sql.Column subcategoryExpr = buildSubcategoryExpression(searchColumn);
            result = result.withColumn(subcategoryColumn, subcategoryExpr);

            // Drop temporary search column
            result = result.drop("_search_text");

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to categorize transactions: " + e.getMessage(), e);
        }
    }

    /**
     * Build category expression based on keywords.
     */
    private org.apache.spark.sql.Column buildCategoryExpression(String searchColumn, String amountColumn) {
        org.apache.spark.sql.Column col = functions.col(searchColumn);

        // Start with WHEN conditions for each category
        org.apache.spark.sql.Column expr = functions
                // INCOME (positive amounts)
                .when(col.contains("salary").or(col.contains("payroll"))
                        .or(col.contains("deposit")).or(col.contains("refund"))
                        .or(col.contains("reimbursement")), "INCOME")

                // GROCERIES
                .when(col.contains("grocery").or(col.contains("supermarket"))
                        .or(col.contains("walmart")).or(col.contains("target"))
                        .or(col.contains("costco")).or(col.contains("whole foods"))
                        .or(col.contains("trader joe")).or(col.contains("safeway"))
                        .or(col.contains("kroger")), "GROCERIES")

                // DINING
                .when(col.contains("restaurant").or(col.contains("cafe"))
                        .or(col.contains("coffee")).or(col.contains("starbucks"))
                        .or(col.contains("mcdonald")).or(col.contains("pizza"))
                        .or(col.contains("doordash")).or(col.contains("ubereats"))
                        .or(col.contains("grubhub")).or(col.contains("dining")), "DINING")

                // TRANSPORTATION
                .when(col.contains("gas").or(col.contains("fuel"))
                        .or(col.contains("shell")).or(col.contains("chevron"))
                        .or(col.contains("uber")).or(col.contains("lyft"))
                        .or(col.contains("parking")).or(col.contains("toll"))
                        .or(col.contains("transit")).or(col.contains("metro")), "TRANSPORTATION")

                // UTILITIES
                .when(col.contains("electric").or(col.contains("power"))
                        .or(col.contains("gas company")).or(col.contains("water"))
                        .or(col.contains("internet")).or(col.contains("phone"))
                        .or(col.contains("comcast")).or(col.contains("at&t"))
                        .or(col.contains("verizon")).or(col.contains("utility")), "UTILITIES")

                // HEALTHCARE
                .when(col.contains("medical").or(col.contains("pharmacy"))
                        .or(col.contains("cvs")).or(col.contains("walgreens"))
                        .or(col.contains("hospital")).or(col.contains("doctor"))
                        .or(col.contains("dentist")).or(col.contains("health"))
                        .or(col.contains("insurance")), "HEALTHCARE")

                // ENTERTAINMENT
                .when(col.contains("movie").or(col.contains("cinema"))
                        .or(col.contains("netflix")).or(col.contains("spotify"))
                        .or(col.contains("hulu")).or(col.contains("disney"))
                        .or(col.contains("amazon prime")).or(col.contains("game"))
                        .or(col.contains("concert")).or(col.contains("theater")), "ENTERTAINMENT")

                // SHOPPING
                .when(col.contains("amazon").or(col.contains("ebay"))
                        .or(col.contains("clothing")).or(col.contains("apparel"))
                        .or(col.contains("retail")).or(col.contains("department store"))
                        .or(col.contains("macy")).or(col.contains("nordstrom")), "SHOPPING")

                // TRAVEL
                .when(col.contains("hotel").or(col.contains("airline"))
                        .or(col.contains("flight")).or(col.contains("airbnb"))
                        .or(col.contains("rental car")).or(col.contains("travel"))
                        .or(col.contains("booking.com")).or(col.contains("expedia")), "TRAVEL")

                // EDUCATION
                .when(col.contains("tuition").or(col.contains("university"))
                        .or(col.contains("college")).or(col.contains("school"))
                        .or(col.contains("books")).or(col.contains("course"))
                        .or(col.contains("udemy")).or(col.contains("coursera")), "EDUCATION")

                // FINANCE
                .when(col.contains("loan").or(col.contains("mortgage"))
                        .or(col.contains("investment")).or(col.contains("credit card"))
                        .or(col.contains("insurance premium")).or(col.contains("payment")), "FINANCE")

                // TRANSFER
                .when(col.contains("transfer").or(col.contains("venmo"))
                        .or(col.contains("paypal")).or(col.contains("zelle"))
                        .or(col.contains("cash app")), "TRANSFER")

                // ATM
                .when(col.contains("atm").or(col.contains("withdrawal"))
                        .or(col.contains("cash")), "ATM")

                // DEFAULT
                .otherwise("OTHER");

        return expr;
    }

    /**
     * Build subcategory expression for more granular classification.
     */
    private org.apache.spark.sql.Column buildSubcategoryExpression(String searchColumn) {
        org.apache.spark.sql.Column col = functions.col(searchColumn);

        return functions
                .when(col.contains("starbucks").or(col.contains("coffee")), "COFFEE")
                .when(col.contains("fast food").or(col.contains("mcdonald"))
                        .or(col.contains("burger king")).or(col.contains("wendy")), "FAST_FOOD")
                .when(col.contains("gas").or(col.contains("fuel")), "FUEL")
                .when(col.contains("streaming"), "STREAMING")
                .when(col.contains("pharmacy"), "PHARMACY")
                .when(col.contains("grocery"), "GROCERY_STORE")
                .otherwise("GENERAL");
    }
}
