#!/bin/bash

################################################################################
# Comprehensive Banking Pipeline - Quick Start Script
#
# This script builds and runs the comprehensive banking pipeline that
# demonstrates all features of the Enterprise Data Pipeline Platform.
#
# Usage:
#   ./run-comprehensive-pipeline.sh [OPTIONS]
#
# Options:
#   --build-only    Only build the project, don't run
#   --skip-build    Skip build and run directly
#   --spark-submit  Use spark-submit instead of Maven exec
#   --help          Show this help message
#
################################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
BUILD_ONLY=false
SKIP_BUILD=false
USE_SPARK_SUBMIT=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --build-only)
            BUILD_ONLY=true
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --spark-submit)
            USE_SPARK_SUBMIT=true
            shift
            ;;
        --help)
            grep '^#' "$0" | grep -v '#!/bin/bash' | sed 's/^# //'
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}  Enterprise Data Pipeline Platform - Comprehensive Banking Pipeline${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

# Check Java
if ! command -v java &> /dev/null; then
    echo -e "${RED}❌ Java not found. Please install Java 17 or higher.${NC}"
    exit 1
fi

JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
if [ "$JAVA_VERSION" -lt 17 ]; then
    echo -e "${RED}❌ Java 17 or higher required. Found: $JAVA_VERSION${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Java $JAVA_VERSION${NC}"

# Check Maven
if ! command -v mvn &> /dev/null; then
    echo -e "${RED}❌ Maven not found. Please install Maven 3.8 or higher.${NC}"
    exit 1
fi
MVN_VERSION=$(mvn -version | grep "Apache Maven" | awk '{print $3}')
echo -e "${GREEN}✓ Maven $MVN_VERSION${NC}"

# Check Spark (optional, only if using spark-submit)
if [ "$USE_SPARK_SUBMIT" = true ]; then
    if ! command -v spark-submit &> /dev/null; then
        echo -e "${YELLOW}⚠ spark-submit not found. Falling back to Maven exec.${NC}"
        USE_SPARK_SUBMIT=false
    else
        SPARK_VERSION=$(spark-submit --version 2>&1 | grep "version" | head -1 | awk '{print $NF}')
        echo -e "${GREEN}✓ Spark $SPARK_VERSION${NC}"
    fi
fi

echo ""

# Navigate to project root
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

echo -e "${BLUE}Project root: $PROJECT_ROOT${NC}"
echo ""

# Build the project
if [ "$SKIP_BUILD" = false ]; then
    echo -e "${YELLOW}Building the project...${NC}"
    echo -e "${BLUE}Running: mvn clean install -DskipTests${NC}"

    if mvn clean install -DskipTests; then
        echo -e "${GREEN}✓ Build successful${NC}"
        echo ""
    else
        echo -e "${RED}❌ Build failed${NC}"
        exit 1
    fi
fi

# Exit if build-only
if [ "$BUILD_ONLY" = true ]; then
    echo -e "${GREEN}Build completed successfully. Exiting (--build-only mode).${NC}"
    exit 0
fi

# Run the pipeline
echo -e "${YELLOW}Running the Comprehensive Banking Pipeline...${NC}"
echo ""

cd "$SCRIPT_DIR"

if [ "$USE_SPARK_SUBMIT" = true ]; then
    echo -e "${BLUE}Using spark-submit${NC}"
    echo -e "${BLUE}Command: spark-submit --class com.enterprise.pipeline.examples.ComprehensiveBankingPipeline ...${NC}"
    echo ""

    spark-submit \
        --class com.enterprise.pipeline.examples.ComprehensiveBankingPipeline \
        --master "local[*]" \
        --driver-memory 4g \
        --conf "spark.sql.adaptive.enabled=true" \
        --conf "spark.ui.showConsoleProgress=false" \
        target/pipeline-examples-1.0-SNAPSHOT.jar
else
    echo -e "${BLUE}Using Maven exec${NC}"
    echo -e "${BLUE}Command: mvn exec:java -Dexec.mainClass=...${NC}"
    echo ""

    mvn exec:java \
        -Dexec.mainClass="com.enterprise.pipeline.examples.ComprehensiveBankingPipeline" \
        -Dexec.cleanupDaemonThreads=false \
        -q
fi

EXIT_CODE=$?

echo ""
echo -e "${BLUE}================================================================================${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Pipeline completed successfully!${NC}"
    echo ""
    echo -e "${GREEN}What was demonstrated:${NC}"
    echo -e "  • Loan Application Processing (validation, enrichment, approval)"
    echo -e "  • Real-Time Fraud Detection"
    echo -e "  • Customer 360 View"
    echo -e "  • Regulatory Compliance (KYC)"
    echo -e "  • Advanced Analytics (Interest Rate Determination)"
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo -e "  1. Review the code: pipeline-examples/src/main/java/.../ComprehensiveBankingPipeline.java"
    echo -e "  2. Customize with your data sources"
    echo -e "  3. Add custom business rules"
    echo -e "  4. Deploy to production cluster"
else
    echo -e "${RED}❌ Pipeline failed with exit code: $EXIT_CODE${NC}"
    echo ""
    echo -e "${YELLOW}Troubleshooting tips:${NC}"
    echo -e "  1. Check the error message above"
    echo -e "  2. Verify all dependencies are installed"
    echo -e "  3. Try rebuilding: ./run-comprehensive-pipeline.sh --build-only"
    echo -e "  4. Check README.md for troubleshooting guide"
fi

echo -e "${BLUE}================================================================================${NC}"

exit $EXIT_CODE
