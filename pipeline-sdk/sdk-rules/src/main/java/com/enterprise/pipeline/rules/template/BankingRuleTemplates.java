package com.enterprise.pipeline.rules.template;

import com.enterprise.pipeline.rules.model.*;

import java.util.List;

/**
 * Pre-built rule templates for common banking scenarios.
 *
 * These templates can be used directly or customized via the UI.
 *
 * @author Enterprise Data Pipeline Team
 */
public class BankingRuleTemplates {

    /**
     * Loan approval rule based on credit score, DTI, and employment.
     */
    public static BusinessRule loanApprovalRule() {
        ConditionGroup conditions = ConditionGroup.and(
                new Condition("credit_score", Operator.GREATER_THAN_OR_EQUAL, 650),
                new Condition("debt_to_income_ratio", Operator.LESS_THAN, 0.43),
                new Condition("employment_status", Operator.IN,
                        List.of("FULL_TIME", "SELF_EMPLOYED", "BUSINESS_OWNER"))
        );

        List<Action> actions = List.of(
                new Action("loan_status", "APPROVED"),
                new Action("approval_reason", "Meets credit and DTI requirements")
        );

        List<Action> elseActions = List.of(
                new Action("loan_status", "REJECTED"),
                Action.withExpression("rejection_reason",
                        "CASE " +
                        "WHEN credit_score < 650 THEN 'Low credit score' " +
                        "WHEN debt_to_income_ratio >= 0.43 THEN 'High debt-to-income ratio' " +
                        "ELSE 'Employment requirement not met' END")
        );

        return BusinessRule.builder()
                .ruleName("loan_approval_decision")
                .description("Approve or reject loan based on credit score, DTI, and employment")
                .conditions(conditions)
                .actions(actions)
                .elseActions(elseActions)
                .build();
    }

    /**
     * Fraud detection rule for suspicious transactions.
     */
    public static ValidationRule fraudDetectionRule() {
        ConditionGroup conditions = ConditionGroup.or(
                // High amount during late night
                ConditionGroup.and(
                        new Condition("amount", Operator.GREATER_THAN, 10000),
                        Condition.between("transaction_hour", 22, 6)
                ),
                // High velocity
                new Condition("velocity_count", Operator.GREATER_THAN, 10),
                // Cross-border high value
                ConditionGroup.and(
                        Condition.compareFields("merchant_country", Operator.NOT_EQUALS, "customer_country"),
                        new Condition("amount", Operator.GREATER_THAN, 1000)
                )
        );

        return ValidationRule.builder()
                .ruleName("fraud_detection")
                .description("Detect potentially fraudulent transactions")
                .conditions(conditions)
                .severity(Severity.CRITICAL)
                .outputColumn("is_suspicious")
                .build();
    }

    /**
     * KYC compliance validation rule.
     */
    public static ValidationRule kycComplianceRule() {
        ConditionGroup conditions = ConditionGroup.and(
                new Condition("id_document", Operator.IS_NOT_NULL, null),
                new Condition("address_proof", Operator.IS_NOT_NULL, null),
                Condition.compareFields("document_expiry_date", Operator.GREATER_THAN, "CURRENT_DATE()"),
                ConditionGroup.or(
                        new Condition("id_type", Operator.EQUALS, "PASSPORT"),
                        new Condition("id_type", Operator.EQUALS, "DRIVERS_LICENSE"),
                        new Condition("id_type", Operator.EQUALS, "NATIONAL_ID")
                )
        );

        return ValidationRule.builder()
                .ruleName("kyc_compliance")
                .description("Validate KYC document completeness and validity")
                .conditions(conditions)
                .severity(Severity.CRITICAL)
                .outputColumn("kyc_compliant")
                .build();
    }

    /**
     * EMI calculation transformation rule.
     */
    public static TransformationRule emiCalculationRule() {
        Expression expression = new Expression(
                "P * r * POWER(1 + r, n) / (POWER(1 + r, n) - 1)",
                java.util.Map.of(
                        "P", "loan_amount",
                        "r", "monthly_interest_rate",
                        "n", "tenure_months"
                )
        );

        return TransformationRule.builder()
                .ruleName("calculate_emi")
                .description("Calculate monthly EMI for loans")
                .expression(expression)
                .outputColumn("monthly_emi")
                .outputType("double")
                .build();
    }

    /**
     * Credit score grading transformation.
     */
    public static TransformationRule creditScoreGradingRule() {
        Expression expression = new Expression(
                "CASE " +
                "WHEN credit_score >= 800 THEN 'EXCELLENT' " +
                "WHEN credit_score >= 740 THEN 'VERY_GOOD' " +
                "WHEN credit_score >= 670 THEN 'GOOD' " +
                "WHEN credit_score >= 580 THEN 'FAIR' " +
                "ELSE 'POOR' END"
        );

        return TransformationRule.builder()
                .ruleName("credit_score_grade")
                .description("Convert credit score to grade (EXCELLENT, VERY_GOOD, GOOD, FAIR, POOR)")
                .expression(expression)
                .outputColumn("credit_grade")
                .outputType("string")
                .build();
    }

    /**
     * High-value transaction filter for compliance monitoring.
     */
    public static FilterRule highValueTransactionFilter() {
        ConditionGroup conditions = ConditionGroup.and(
                new Condition("amount", Operator.GREATER_THAN_OR_EQUAL, 10000),
                new Condition("transaction_type", Operator.IN,
                        List.of("WIRE", "TRANSFER", "WITHDRAWAL"))
        );

        return FilterRule.builder()
                .ruleName("high_value_transactions")
                .description("Filter transactions over $10,000 for compliance monitoring")
                .conditions(conditions)
                .build();
    }

    /**
     * Interest rate determination based on credit score.
     */
    public static TransformationRule interestRateDeterminationRule() {
        Expression expression = new Expression(
                "CASE " +
                "WHEN credit_score >= 750 THEN 0.05 " +
                "WHEN credit_score >= 650 THEN 0.07 " +
                "WHEN credit_score >= 550 THEN 0.10 " +
                "ELSE 0.15 END"
        );

        return TransformationRule.builder()
                .ruleName("determine_interest_rate")
                .description("Determine interest rate based on credit score")
                .expression(expression)
                .outputColumn("interest_rate")
                .outputType("double")
                .build();
    }

    /**
     * Loan eligibility validation.
     */
    public static ValidationRule loanEligibilityRule() {
        ConditionGroup conditions = ConditionGroup.and(
                Condition.between("age", 21, 65),
                new Condition("credit_score", Operator.GREATER_THAN_OR_EQUAL, 600),
                new Condition("monthly_income", Operator.GREATER_THAN_OR_EQUAL, 3000),
                new Condition("employment_type", Operator.IN,
                        List.of("SALARIED", "SELF_EMPLOYED", "BUSINESS_OWNER")),
                new Condition("bankruptcy_flag", Operator.EQUALS, false)
        );

        return ValidationRule.builder()
                .ruleName("loan_eligibility")
                .description("Validate basic loan eligibility criteria")
                .conditions(conditions)
                .severity(Severity.ERROR)
                .outputColumn("is_eligible")
                .build();
    }

    /**
     * Transaction amount validation.
     */
    public static ValidationRule transactionAmountValidationRule() {
        ConditionGroup conditions = ConditionGroup.and(
                new Condition("amount", Operator.GREATER_THAN, 0),
                new Condition("amount", Operator.LESS_THAN_OR_EQUAL, 1000000)
        );

        return ValidationRule.builder()
                .ruleName("validate_transaction_amount")
                .description("Ensure transaction amount is within valid range (0 < amount <= 1M)")
                .conditions(conditions)
                .severity(Severity.ERROR)
                .outputColumn("amount_valid")
                .build();
    }

    /**
     * Get all available banking templates.
     */
    public static List<Rule> getAllTemplates() {
        return List.of(
                loanApprovalRule(),
                fraudDetectionRule(),
                kycComplianceRule(),
                emiCalculationRule(),
                creditScoreGradingRule(),
                highValueTransactionFilter(),
                interestRateDeterminationRule(),
                loanEligibilityRule(),
                transactionAmountValidationRule()
        );
    }
}
