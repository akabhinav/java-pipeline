@echo off
REM ============================================================================
REM Initialize All Services - Upload Data to MinIO and Create Kafka Topics
REM ============================================================================

echo ================================================================================
echo   Initializing Services - MinIO ^& Kafka
echo ================================================================================
echo.

REM Wait for services
echo Waiting for services to be ready...
timeout /t 5 /nobreak >nul

REM ============================================================================
REM Initialize MinIO (S3)
REM ============================================================================
echo Step 1: Uploading data to MinIO (S3)...

REM Configure mc (MinIO client)
docker exec pipeline-minio-init mc alias set myminio http://minio:9000 minioadmin minioadmin 2>nul

REM Create Data Lake bucket for Delta Lake
echo   Creating data-lake bucket...
docker exec pipeline-minio-init mc mb myminio/data-lake --ignore-existing
docker exec pipeline-minio-init mc anonymous set download myminio/data-lake

REM Copy data files to MinIO container
echo   Copying customers.csv...
docker cp docker\minio\data\customers.csv pipeline-minio:/tmp/customers.csv
docker exec pipeline-minio-init mc cp /tmp/customers.csv myminio/banking-data/customers/customers.csv

echo   Copying credit_bureau.csv...
docker cp docker\minio\data\credit_bureau.csv pipeline-minio:/tmp/credit_bureau.csv
docker exec pipeline-minio-init mc cp /tmp/credit_bureau.csv myminio/banking-data/credit-bureau/credit_bureau.csv

echo [OK] Data uploaded to MinIO successfully!
echo [OK] Data Lake bucket created (s3a://data-lake/)
echo.

REM ============================================================================
REM Initialize Kafka Topics
REM ============================================================================
echo Step 2: Creating Kafka topics...

docker exec pipeline-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic loan-applications

docker exec pipeline-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic transactions

docker exec pipeline-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic loan-approval-results

docker exec pipeline-kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic fraud-detection-results

echo [OK] Kafka topics created successfully!
echo.

REM List topics
echo Kafka Topics:
docker exec pipeline-kafka kafka-topics --list --bootstrap-server localhost:9092

echo.
echo ================================================================================
echo [OK] All services initialized successfully!
echo ================================================================================
echo.

echo Service URLs:
echo   Kafka UI:        http://localhost:8080
echo   MinIO Console:   http://localhost:9001  (minioadmin/minioadmin)
echo   pgAdmin:         http://localhost:5050  (admin@pipeline.com/admin123)
echo   PostgreSQL:      localhost:5432         (pipeline/pipeline123)
echo   Kafka:           localhost:9092
echo   Spark Master:    http://localhost:8081  (Cluster UI)
echo   Spark Worker 1:  http://localhost:8082
echo   Spark Worker 2:  http://localhost:8083
echo   Spark History:   http://localhost:18080
echo.
