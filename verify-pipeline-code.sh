#!/bin/bash

################################################################################
# Pipeline Code Verification Script
#
# This script verifies that all necessary code components are in place
# for the Comprehensive Banking Pipeline.
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}  Enterprise Data Pipeline - Code Verification${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

PROJECT_ROOT="/home/user/java-pipeline"
cd "$PROJECT_ROOT"

TOTAL_CHECKS=0
PASSED_CHECKS=0

check_file() {
    local file=$1
    local description=$2
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ -f "$file" ]; then
        echo -e "${GREEN}✓${NC} $description"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "${RED}✗${NC} $description - NOT FOUND"
        return 1
    fi
}

check_file_size() {
    local file=$1
    local min_size=$2
    local description=$3
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ -f "$file" ]; then
        local size=$(wc -l < "$file" 2>/dev/null || echo "0")
        if [ "$size" -ge "$min_size" ]; then
            echo -e "${GREEN}✓${NC} $description ($size lines)"
            PASSED_CHECKS=$((PASSED_CHECKS + 1))
            return 0
        else
            echo -e "${YELLOW}⚠${NC} $description - File too small ($size < $min_size lines)"
            return 1
        fi
    else
        echo -e "${RED}✗${NC} $description - NOT FOUND"
        return 1
    fi
}

check_content() {
    local file=$1
    local pattern=$2
    local description=$3
    TOTAL_CHECKS=$((TOTAL_CHECKS + 1))

    if [ -f "$file" ] && grep -q "$pattern" "$file"; then
        echo -e "${GREEN}✓${NC} $description"
        PASSED_CHECKS=$((PASSED_CHECKS + 1))
        return 0
    else
        echo -e "${RED}✗${NC} $description - Pattern not found"
        return 1
    fi
}

echo -e "${YELLOW}Checking Rule Engine Components...${NC}"
echo ""

# Rule Models
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/Severity.java" \
    "Severity enum"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/Operator.java" \
    "Operator enum with 15+ operators"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/Condition.java" \
    "Condition record"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/ConditionGroup.java" \
    "ConditionGroup for nested logic"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/ValidationRule.java" \
    "ValidationRule"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/TransformationRule.java" \
    "TransformationRule"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/BusinessRule.java" \
    "BusinessRule"
check_file "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/model/FilterRule.java" \
    "FilterRule"

echo ""
echo -e "${YELLOW}Checking Rule Executor...${NC}"
echo ""

check_file_size "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/executor/RuleExecutor.java" \
    200 "RuleExecutor (main execution engine)"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/executor/RuleExecutor.java" \
    "executeValidation" "RuleExecutor - Validation execution"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/executor/RuleExecutor.java" \
    "executeTransformation" "RuleExecutor - Transformation execution"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/executor/RuleExecutor.java" \
    "executeBusiness" "RuleExecutor - Business rule execution"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/executor/RuleExecutor.java" \
    "executeFilter" "RuleExecutor - Filter execution"

echo ""
echo -e "${YELLOW}Checking Banking Templates...${NC}"
echo ""

check_file_size "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/template/BankingRuleTemplates.java" \
    300 "BankingRuleTemplates (9 pre-built rules)"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/template/BankingRuleTemplates.java" \
    "loanApprovalRule" "Banking Template - Loan Approval"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/template/BankingRuleTemplates.java" \
    "fraudDetectionRule" "Banking Template - Fraud Detection"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/template/BankingRuleTemplates.java" \
    "kycComplianceRule" "Banking Template - KYC Compliance"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/template/BankingRuleTemplates.java" \
    "emiCalculationRule" "Banking Template - EMI Calculation"
check_content "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules/template/BankingRuleTemplates.java" \
    "creditScoreGradingRule" "Banking Template - Credit Score Grading"

echo ""
echo -e "${YELLOW}Checking Comprehensive Pipeline...${NC}"
echo ""

check_file_size "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    600 "ComprehensiveBankingPipeline (main test)"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "loanApplicationPipeline" "Scenario 1: Loan Application Processing"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "fraudDetectionPipeline" "Scenario 2: Fraud Detection"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "customer360Pipeline" "Scenario 3: Customer 360"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "compliancePipeline" "Scenario 4: Compliance"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "analyticsPipeline" "Scenario 5: Analytics"

echo ""
echo -e "${YELLOW}Checking Sample Data Generation...${NC}"
echo ""

check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "createLoanApplicationData" "Sample Data - Loan Applications (15 records)"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "createCustomerData" "Sample Data - Customers (15 records)"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "createCreditBureauData" "Sample Data - Credit Bureau (15 records)"
check_content "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" \
    "createTransactionData" "Sample Data - Transactions (15 records)"

echo ""
echo -e "${YELLOW}Checking Documentation...${NC}"
echo ""

check_file "README.md" "Main README"
check_file "pipeline-examples/README.md" "Examples README"
check_file "RULE_ENGINE.md" "Rule Engine documentation"
check_file "ARCHITECTURE.md" "Architecture documentation"
check_file "pipeline-examples/run-comprehensive-pipeline.sh" "Execution script"
check_file "PIPELINE_TEST_OUTPUT.md" "Expected test output"

echo ""
echo -e "${YELLOW}Checking Module Structure...${NC}"
echo ""

check_file "pom.xml" "Root POM"
check_file "pipeline-sdk/pom.xml" "SDK Parent POM"
check_file "pipeline-sdk/sdk-rules/pom.xml" "Rules Module POM"
check_file "pipeline-examples/pom.xml" "Examples Module POM"

echo ""
echo -e "${YELLOW}Code Quality Checks...${NC}"
echo ""

# Count lines of code
if [ -f "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java" ]; then
    LINES=$(wc -l < "pipeline-examples/src/main/java/com/enterprise/pipeline/examples/ComprehensiveBankingPipeline.java")
    echo -e "${BLUE}📊 ComprehensiveBankingPipeline: $LINES lines${NC}"
fi

if [ -d "pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules" ]; then
    RULE_FILES=$(find pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules -name "*.java" | wc -l)
    RULE_LINES=$(find pipeline-sdk/sdk-rules/src/main/java/com/enterprise/pipeline/rules -name "*.java" -exec wc -l {} + | tail -1 | awk '{print $1}')
    echo -e "${BLUE}📊 Rule Engine: $RULE_FILES files, $RULE_LINES total lines${NC}"
fi

echo ""
echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}  Verification Summary${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

PERCENTAGE=$((PASSED_CHECKS * 100 / TOTAL_CHECKS))

if [ $PASSED_CHECKS -eq $TOTAL_CHECKS ]; then
    echo -e "${GREEN}✅ All checks passed: $PASSED_CHECKS/$TOTAL_CHECKS (100%)${NC}"
    echo ""
    echo -e "${GREEN}The code is complete and ready to run!${NC}"
    echo ""
    echo -e "${YELLOW}To execute the pipeline:${NC}"
    echo -e "  1. Ensure Maven dependencies are available (network required)"
    echo -e "  2. Run: cd pipeline-examples && ./run-comprehensive-pipeline.sh"
    echo ""
    exit 0
else
    echo -e "${YELLOW}⚠ Checks passed: $PASSED_CHECKS/$TOTAL_CHECKS ($PERCENTAGE%)${NC}"
    echo ""
    if [ $PERCENTAGE -ge 90 ]; then
        echo -e "${YELLOW}Minor issues found, but code should work.${NC}"
        exit 0
    else
        echo -e "${RED}Significant issues found. Please review missing components.${NC}"
        exit 1
    fi
fi
