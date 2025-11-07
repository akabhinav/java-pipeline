package com.enterprise.pipeline.quality.examples;

import com.enterprise.pipeline.quality.DataQualityFramework;
import com.enterprise.pipeline.quality.report.QualityReport;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 * Comprehensive example demonstrating all Data Quality Framework features.
 */
public class DataQualityExample {

    public static void main(String[] args) {
        // Create Spark session
        SparkSession spark = SparkSession.builder()
            .appName("Data Quality Framework Example")
            .master("local[*]")
            .getOrCreate();

        try {
            System.out.println("\n" + "=".repeat(100));
            System.out.println("DATA QUALITY FRAMEWORK - COMPREHENSIVE EXAMPLE");
            System.out.println("=".repeat(100) + "\n");

            // Create sample dataset
            Dataset<Row> customerData = createSampleData(spark);

            System.out.println("Sample Data:");
            customerData.show(10, false);

            // Example 1: Complete Assessment with all features
            System.out.println("\n" + "=".repeat(100));
            System.out.println("EXAMPLE 1: Complete Quality Assessment");
            System.out.println("=".repeat(100));

            QualityReport report = DataQualityFramework.assess(customerData, "customer_data")
                .profile()
                .withMetrics(metrics -> metrics
                    .addCompleteness("customer_id")
                    .addCompleteness("email")
                    .addCompleteness("age")
                    .addUniqueness("customer_id")
                    .addUniqueness("email")
                    .addAccuracy("age", "age >= 18 AND age <= 120")
                    .addAccuracy("email", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'")
                    .addConsistency("registration_date <= last_purchase_date")
                )
                .withRules(rules -> rules
                    .addNotNull("customer_id")
                    .addNotNull("email")
                    .addRange("age", 18, 120)
                    .addRegex("email", "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
                    .addUnique("customer_id")
                )
                .detectAnomalies()
                .detectVolumeAnomalies(1000, 20.0) // Expected 1000 rows ±20%
                .generate();

            System.out.println(report);

            // Example 2: Quick Quality Check
            System.out.println("\n" + "=".repeat(100));
            System.out.println("EXAMPLE 2: Quick Quality Check (Pass/Fail)");
            System.out.println("=".repeat(100));

            boolean passesQuality = DataQualityFramework.assess(customerData, "customer_data")
                .withMetrics(metrics -> metrics
                    .addCompleteness("email")
                    .addUniqueness("customer_id")
                )
                .withRules(rules -> rules
                    .addNotNull("customer_id")
                    .addNotNull("email")
                )
                .quickCheck(80.0); // 80% threshold

            System.out.println("Quality Check Result: " + (passesQuality ? "✓ PASSED" : "✗ FAILED"));

            // Example 3: Profiling Only
            System.out.println("\n" + "=".repeat(100));
            System.out.println("EXAMPLE 3: Data Profiling Only");
            System.out.println("=".repeat(100));

            QualityReport profileReport = DataQualityFramework.assess(customerData, "customer_data")
                .profile()
                .generate();

            System.out.println(profileReport);

            // Example 4: Rules Validation Only
            System.out.println("\n" + "=".repeat(100));
            System.out.println("EXAMPLE 4: Rules Validation Only");
            System.out.println("=".repeat(100));

            QualityReport rulesReport = DataQualityFramework.assess(customerData, "customer_data")
                .withRules(rules -> rules
                    .addNotNull("customer_id")
                    .addNotNull("email")
                    .addRange("age", 0, 150)
                    .addUnique("customer_id")
                )
                .generate();

            System.out.println(rulesReport);

            // Example 5: Complete Assessment Shortcut
            System.out.println("\n" + "=".repeat(100));
            System.out.println("EXAMPLE 5: Complete Assessment (One-Liner)");
            System.out.println("=".repeat(100));

            QualityReport quickReport = DataQualityFramework.completeAssessment(
                customerData, "customer_data"
            );

            System.out.println("Overall Quality Score: " + String.format("%.2f%%",
                quickReport.getOverallScore()));

            System.out.println("\n" + "=".repeat(100));
            System.out.println("ALL EXAMPLES COMPLETED SUCCESSFULLY");
            System.out.println("=".repeat(100) + "\n");

        } finally {
            spark.stop();
        }
    }

    private static Dataset<Row> createSampleData(SparkSession spark) {
        // Create realistic sample data with some quality issues
        return spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("data:text/csv," +
                "customer_id,name,email,age,city,registration_date,last_purchase_date,total_purchases\n" +
                "C001,John Doe,john.doe@email.com,35,New York,2023-01-15,2024-05-20,1500.50\n" +
                "C002,Jane Smith,jane.smith@email.com,28,Los Angeles,2023-03-10,2024-06-15,2300.75\n" +
                "C003,Bob Johnson,bob.johnson@invalid,42,Chicago,2023-02-20,2024-04-30,800.00\n" +  // Invalid email
                "C004,Alice Williams,,31,Houston,2023-04-05,2024-07-10,1200.25\n" +  // Missing email
                "C005,Charlie Brown,charlie.brown@email.com,150,Phoenix,2023-05-12,2024-08-05,950.00\n" +  // Invalid age
                "C006,Diana Prince,diana.prince@email.com,29,Philadelphia,2023-06-01,2024-09-12,1750.80\n" +
                "C007,Edward Norton,edward.norton@email.com,38,San Antonio,2023-07-15,2024-10-20,3200.00\n" +
                "C008,Fiona Green,,45,San Diego,2023-08-20,2024-11-05,450.30\n" +  // Missing email
                "C009,George Hill,george.hill@email.com,52,Dallas,2023-09-10,2024-12-15,1850.60\n" +
                "C010,Hannah White,hannah.white@email.com,33,San Jose,2023-10-05,2024-01-20,1100.90\n" +
                "C011,Ian Black,ian.black@email.com,27,Austin,2023-11-12,2024-02-25,2100.40\n" +
                "C012,Julia Green,julia.green@email.com,41,Jacksonville,2023-12-20,2024-03-30,980.50\n" +
                "C001,John Duplicate,john.duplicate@email.com,36,Boston,2024-01-10,2024-04-15,500.00\n" +  // Duplicate ID
                "C014,Kevin Lee,kevin.lee@email.com,-5,Seattle,2024-02-05,2024-05-10,1300.75\n" +  // Negative age
                "C015,Laura Martinez,laura.martinez@email.com,39,Denver,2024-03-15,2024-06-20,1650.20\n"
            );
    }
}
