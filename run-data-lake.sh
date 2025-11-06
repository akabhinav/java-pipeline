#!/bin/bash

################################################################################
# Run Data Lake Pipeline
#
# This script runs the Data Lake pipeline demonstrating:
# - Medallion Architecture (Bronze/Silver/Gold)
# - Delta Lake features (ACID, Time Travel, Schema Evolution)
# - MinIO as S3-compatible storage
#
# Prerequisites:
#   - Local environment running: ./start-local-environment.sh
#   - MinIO available at: http://localhost:9000
#
# Usage:
#   ./run-data-lake.sh
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}  🏗️  Data Lake Pipeline - Medallion Architecture${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

# Check if MinIO is running
echo -e "${YELLOW}Checking prerequisites...${NC}"
if ! docker ps | grep -q pipeline-minio; then
    echo -e "${RED}❌ MinIO not running!${NC}"
    echo -e "${YELLOW}Start the environment first: ./start-local-environment.sh${NC}"
    exit 1
fi

echo -e "${GREEN}✓ MinIO is running${NC}"
echo ""

# Show what will happen
echo -e "${CYAN}This pipeline will:${NC}"
echo -e "  ${CYAN}1.${NC} Write raw data to Bronze Layer (s3a://data-lake/bronze/)"
echo -e "  ${CYAN}2.${NC} Clean and validate to Silver Layer (s3a://data-lake/silver/)"
echo -e "  ${CYAN}3.${NC} Aggregate to Gold Layer (s3a://data-lake/gold/)"
echo -e "  ${CYAN}4.${NC} Demonstrate Delta Lake features:"
echo -e "     • ACID transactions"
echo -e "     • Time travel"
echo -e "     • Upserts (merge)"
echo -e "     • Schema evolution"
echo ""

echo -e "${YELLOW}Running Data Lake pipeline...${NC}"
echo ""

cd pipeline-examples

mvn exec:java \
    -Dexec.mainClass="com.enterprise.pipeline.examples.DataLakePipeline" \
    -Dexec.cleanupDaemonThreads=false

EXIT_CODE=$?

echo ""
echo -e "${BLUE}================================================================================${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Data Lake Pipeline completed successfully!${NC}"
    echo ""
    echo -e "${CYAN}View your Data Lake:${NC}"
    echo -e "  ${CYAN}MinIO Console:${NC}  http://localhost:9001"
    echo -e "  ${CYAN}Bucket:${NC}         data-lake"
    echo -e "  ${CYAN}Layers:${NC}         bronze/, silver/, gold/"
    echo ""
    echo -e "${CYAN}Query your data:${NC}"
    echo -e "  spark.read().format(\"delta\").load(\"s3a://data-lake/gold/loan_summary_by_purpose\")"
    echo ""
    echo -e "${YELLOW}📖 Read DATA_LAKE_GUIDE.md for comprehensive documentation${NC}"
else
    echo -e "${RED}❌ Pipeline failed with exit code: $EXIT_CODE${NC}"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo -e "  1. Check MinIO is running:     docker ps | grep minio"
    echo -e "  2. Verify data-lake bucket:    http://localhost:9001"
    echo -e "  3. Check logs above for errors"
fi

echo -e "${BLUE}================================================================================${NC}"

exit $EXIT_CODE
