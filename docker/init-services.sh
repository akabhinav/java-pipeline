#!/bin/bash

################################################################################
# Initialize All Services - Upload Data to MinIO and Create Kafka Topics
################################################################################

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}================================================================================${NC}"
echo -e "${BLUE}  Initializing Services - MinIO & Kafka${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

# Wait for services to be ready
echo -e "${YELLOW}Waiting for services to be ready...${NC}"
sleep 10

# ==============================================================================
# Initialize MinIO (S3)
# ==============================================================================
echo -e "${BLUE}Step 1: Uploading data to MinIO (S3)...${NC}"

# Configure mc (MinIO client)
docker exec pipeline-minio-init mc alias set myminio http://minio:9000 minioadmin minioadmin || true

# Create Data Lake bucket for Delta Lake
echo "  Creating data-lake bucket..."
docker exec pipeline-minio-init mc mb myminio/data-lake --ignore-existing
docker exec pipeline-minio-init mc anonymous set download myminio/data-lake

# Copy data files to MinIO container
echo "  Copying customers.csv..."
docker cp docker/minio/data/customers.csv pipeline-minio:/tmp/customers.csv
docker exec pipeline-minio-init mc cp /tmp/customers.csv myminio/banking-data/customers/customers.csv

echo "  Copying credit_bureau.csv..."
docker cp docker/minio/data/credit_bureau.csv pipeline-minio:/tmp/credit_bureau.csv
docker exec pipeline-minio-init mc cp /tmp/credit_bureau.csv myminio/banking-data/credit-bureau/credit_bureau.csv

echo -e "${GREEN}✓ Data uploaded to MinIO successfully!${NC}"
echo -e "${GREEN}✓ Data Lake bucket created (s3a://data-lake/)${NC}"
echo ""

# ==============================================================================
# Initialize Kafka Topics
# ==============================================================================
echo -e "${BLUE}Step 2: Creating Kafka topics...${NC}"

# Create topics
docker exec pipeline-kafka kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic loan-applications

docker exec pipeline-kafka kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic transactions

docker exec pipeline-kafka kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic loan-approval-results

docker exec pipeline-kafka kafka-topics \
    --create --if-not-exists \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic fraud-detection-results

echo -e "${GREEN}✓ Kafka topics created successfully!${NC}"
echo ""

# List topics
echo -e "${BLUE}Kafka Topics:${NC}"
docker exec pipeline-kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo -e "${BLUE}================================================================================${NC}"
echo -e "${GREEN}✅ All services initialized successfully!${NC}"
echo -e "${BLUE}================================================================================${NC}"
echo ""

echo -e "${YELLOW}Service URLs:${NC}"
echo -e "  📊 Kafka UI:        http://localhost:8080"
echo -e "  🗄️  MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)"
echo -e "  🐘 pgAdmin:        http://localhost:5050  (admin@pipeline.com/admin123)"
echo -e "  🗃️  PostgreSQL:     localhost:5432         (pipeline/pipeline123)"
echo -e "  📡 Kafka:          localhost:9092"
echo -e "  ⚡ Spark Master:    http://localhost:8081  (Cluster UI)"
echo -e "  ⚡ Spark Worker 1:  http://localhost:8082"
echo -e "  ⚡ Spark Worker 2:  http://localhost:8083"
echo -e "  📊 Spark History:  http://localhost:18080"
echo ""
