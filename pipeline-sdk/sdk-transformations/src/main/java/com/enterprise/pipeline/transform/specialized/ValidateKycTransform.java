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
 * Validate KYC (Know Your Customer) compliance for customer records.
 *
 * Validates the following KYC requirements:
 * - Full name present
 * - Date of birth present and customer is adult (18+)
 * - Valid government ID (SSN, passport, driver's license)
 * - Valid address information
 * - Valid phone number
 * - Valid email address
 *
 * Config:
 * - nameColumn: Column containing customer name (required)
 * - dobColumn: Column containing date of birth (optional)
 * - ssnColumn: Column containing SSN (optional)
 * - passportColumn: Column containing passport number (optional)
 * - addressColumn: Column containing address (optional)
 * - phoneColumn: Column containing phone number (optional)
 * - emailColumn: Column containing email (optional)
 * - validationColumn: Output column for validation result (default: "kyc_valid")
 * - validationDetailsColumn: Output column for validation details (default: "kyc_validation_details")
 * - complianceLevelColumn: Output column for compliance level (default: "kyc_compliance_level")
 *
 * Compliance Levels:
 * - FULL: All required fields present and valid
 * - PARTIAL: Some required fields present
 * - INSUFFICIENT: Critical fields missing
 *
 * Example:
 * {
 *   "type": "validateKyc",
 *   "config": {
 *     "nameColumn": "full_name",
 *     "dobColumn": "date_of_birth",
 *     "ssnColumn": "ssn",
 *     "addressColumn": "address",
 *     "phoneColumn": "phone",
 *     "emailColumn": "email",
 *     "validationColumn": "kyc_compliant"
 *   }
 * }
 *
 * @author Enterprise Data Pipeline Team
 */
public class ValidateKycTransform implements Transformation {

    private static final Logger logger = LoggerFactory.getLogger(ValidateKycTransform.class);
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
        return "validateKyc";
    }

    @Override
    public void validate(Map<String, Object> config) throws ValidationException {
        if (!config.containsKey("nameColumn")) {
            throw new ValidationException("'nameColumn' is required");
        }
    }

    @Override
    public Dataset<Row> transform(Dataset<Row> input, Map<String, Object> config, TransformationContext context)
            throws TransformationException {
        try {
            String nameColumn = (String) config.get("nameColumn");
            String validationColumn = config.containsKey("validationColumn")
                    ? (String) config.get("validationColumn")
                    : "kyc_valid";
            String validationDetailsColumn = config.containsKey("validationDetailsColumn")
                    ? (String) config.get("validationDetailsColumn")
                    : "kyc_validation_details";
            String complianceLevelColumn = config.containsKey("complianceLevelColumn")
                    ? (String) config.get("complianceLevelColumn")
                    : "kyc_compliance_level";

            logger.debug("Validating KYC compliance");

            Dataset<Row> result = input;

            // Validation flags
            org.apache.spark.sql.Column hasName = functions.col(nameColumn).isNotNull()
                    .and(functions.length(functions.trim(functions.col(nameColumn))).$greater(0));

            // Date of birth validation (must be 18+ years old)
            org.apache.spark.sql.Column hasValidDob = functions.lit(true);
            if (config.containsKey("dobColumn")) {
                String dobColumn = (String) config.get("dobColumn");
                hasValidDob = functions.col(dobColumn).isNotNull()
                        .and(functions.months_between(functions.current_date(), functions.col(dobColumn)).$greater(216)); // 18 years * 12 months
                logger.debug("Including DOB validation");
            }

            // Government ID validation (SSN or Passport)
            org.apache.spark.sql.Column hasGovId = functions.lit(false);
            if (config.containsKey("ssnColumn")) {
                String ssnColumn = (String) config.get("ssnColumn");
                org.apache.spark.sql.Column hasValidSsn = functions.col(ssnColumn).isNotNull()
                        .and(functions.col(ssnColumn).rlike("^[0-9]{3}-?[0-9]{2}-?[0-9]{4}$"));
                hasGovId = hasGovId.or(hasValidSsn);
                logger.debug("Including SSN validation");
            }
            if (config.containsKey("passportColumn")) {
                String passportColumn = (String) config.get("passportColumn");
                org.apache.spark.sql.Column hasValidPassport = functions.col(passportColumn).isNotNull()
                        .and(functions.length(functions.col(passportColumn)).$greater$eq(6));
                hasGovId = hasGovId.or(hasValidPassport);
                logger.debug("Including passport validation");
            }

            // Address validation
            org.apache.spark.sql.Column hasAddress = functions.lit(true);
            if (config.containsKey("addressColumn")) {
                String addressColumn = (String) config.get("addressColumn");
                hasAddress = functions.col(addressColumn).isNotNull()
                        .and(functions.length(functions.trim(functions.col(addressColumn))).$greater(10));
                logger.debug("Including address validation");
            }

            // Phone validation
            org.apache.spark.sql.Column hasPhone = functions.lit(true);
            if (config.containsKey("phoneColumn")) {
                String phoneColumn = (String) config.get("phoneColumn");
                hasPhone = functions.col(phoneColumn).isNotNull()
                        .and(functions.col(phoneColumn).rlike("^[+]?[0-9]{10,15}$"));
                logger.debug("Including phone validation");
            }

            // Email validation
            org.apache.spark.sql.Column hasEmail = functions.lit(true);
            if (config.containsKey("emailColumn")) {
                String emailColumn = (String) config.get("emailColumn");
                hasEmail = functions.col(emailColumn).isNotNull()
                        .and(functions.col(emailColumn).rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"));
                logger.debug("Including email validation");
            }

            // Calculate validation score (how many checks passed)
            org.apache.spark.sql.Column validationScore = functions.lit(0)
                    .plus(functions.when(hasName, 1).otherwise(0))
                    .plus(functions.when(hasValidDob, 1).otherwise(0))
                    .plus(functions.when(hasGovId, 1).otherwise(0))
                    .plus(functions.when(hasAddress, 1).otherwise(0))
                    .plus(functions.when(hasPhone, 1).otherwise(0))
                    .plus(functions.when(hasEmail, 1).otherwise(0));

            // Overall validation: name + gov_id + at least 2 other fields
            org.apache.spark.sql.Column isValid = hasName
                    .and(hasGovId)
                    .and(validationScore.$greater$eq(4));

            result = result.withColumn(validationColumn, isValid);

            // Compliance level
            result = result.withColumn(complianceLevelColumn,
                    functions.when(validationScore.$greater$eq(5), "FULL")
                            .when(validationScore.$greater$eq(3), "PARTIAL")
                            .otherwise("INSUFFICIENT")
            );

            // Validation details
            result = result.withColumn(validationDetailsColumn,
                    functions.concat_ws(", ",
                            functions.when(hasName, functions.lit("NAME_OK")).otherwise(functions.lit("NAME_MISSING")),
                            functions.when(hasValidDob, functions.lit("DOB_OK")).otherwise(functions.lit("DOB_INVALID")),
                            functions.when(hasGovId, functions.lit("GOV_ID_OK")).otherwise(functions.lit("GOV_ID_MISSING")),
                            functions.when(hasAddress, functions.lit("ADDRESS_OK")).otherwise(functions.lit("ADDRESS_INVALID")),
                            functions.when(hasPhone, functions.lit("PHONE_OK")).otherwise(functions.lit("PHONE_INVALID")),
                            functions.when(hasEmail, functions.lit("EMAIL_OK")).otherwise(functions.lit("EMAIL_INVALID"))
                    )
            );

            return result;

        } catch (Exception e) {
            throw new TransformationException(getName(),
                    "Failed to validate KYC: " + e.getMessage(), e);
        }
    }
}
