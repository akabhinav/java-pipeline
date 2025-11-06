#!/bin/bash

################################################################################
# Run Pipeline on Spark Cluster
#
# This script submits a pipeline job to the Spark cluster running in Docker.
#
# Usage:
#   ./run-pipeline-on-cluster.sh [pipeline-class-name]
#
# Examples:
#   ./run-pipeline-on-cluster.sh RealConnectorsPipeline
#   ./run-pipeline-on-cluster.sh ComprehensiveBankingPipeline
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Default pipeline class
PIPELINE_CLASS="${1:-RealConnectorsPipeline}"
FULL_CLASS_NAME="com.enterprise.pipeline.examples.${PIPELINE_CLASS}"

echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}  Running Pipeline on Spark Cluster${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

# Check if Spark cluster is running
echo -e "${YELLOW}Checking Spark cluster status...${NC}"
if ! docker ps | grep -q pipeline-spark-master; then
    echo -e "${RED}❌ Spark Master not running!${NC}"
    echo -e "${YELLOW}Start the environment first: ./start-local-environment.sh${NC}"
    exit 1
fi

if ! docker ps | grep -q pipeline-spark-worker; then
    echo -e "${RED}❌ Spark Workers not running!${NC}"
    echo -e "${YELLOW}Start the environment first: ./start-local-environment.sh${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Spark cluster is running${NC}"
echo ""

# Check if JAR exists
JAR_FILE="pipeline-examples/target/pipeline-examples-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${YELLOW}Building project...${NC}"
    mvn clean package -DskipTests -q
    echo -e "${GREEN}✓ Build complete${NC}"
else
    echo -e "${GREEN}✓ JAR file found: $JAR_FILE${NC}"
fi
echo ""

# Submit to cluster
echo -e "${BLUE}Submitting job to Spark cluster...${NC}"
echo -e "${YELLOW}Pipeline Class: ${FULL_CLASS_NAME}${NC}"
echo -e "${YELLOW}Spark Master:   spark://localhost:7077${NC}"
echo ""

docker exec pipeline-spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --class "${FULL_CLASS_NAME}" \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --total-executor-cores 4 \
    --conf spark.sql.adaptive.enabled=true \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    /opt/spark-apps/pipeline-examples-1.0-SNAPSHOT.jar

EXIT_CODE=$?

echo ""
echo -e "${BLUE}================================================================================${NC}"

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Pipeline completed successfully on Spark cluster!${NC}"
    echo ""
    echo -e "${CYAN}View cluster details:${NC}"
    echo -e "  Spark Master UI:    http://localhost:8081"
    echo -e "  Worker 1 UI:        http://localhost:8082"
    echo -e "  Worker 2 UI:        http://localhost:8083"
    echo -e "  History Server:     http://localhost:18080"
else
    echo -e "${RED}❌ Pipeline failed with exit code: $EXIT_CODE${NC}"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo -e "  1. Check Spark Master logs:  docker logs pipeline-spark-master"
    echo -e "  2. Check Worker logs:         docker logs pipeline-spark-worker-1"
    echo -e "  3. View Spark UI:             http://localhost:8081"
fi

echo -e "${BLUE}================================================================================${NC}"

exit $EXIT_CODE
