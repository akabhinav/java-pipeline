@echo off
REM ============================================================================
REM Run Pipeline on Spark Cluster (Windows)
REM
REM This script submits a pipeline job to the Spark cluster running in Docker.
REM
REM Usage:
REM   run-pipeline-on-cluster.bat [pipeline-class-name]
REM
REM Examples:
REM   run-pipeline-on-cluster.bat RealConnectorsPipeline
REM   run-pipeline-on-cluster.bat ComprehensiveBankingPipeline
REM ============================================================================

setlocal enabledelayedexpansion

set PIPELINE_CLASS=%1
if "%PIPELINE_CLASS%"=="" set PIPELINE_CLASS=RealConnectorsPipeline
set FULL_CLASS_NAME=com.enterprise.pipeline.examples.%PIPELINE_CLASS%

echo ================================================================================
echo   Running Pipeline on Spark Cluster
echo ================================================================================
echo.

REM Check if Spark cluster is running
echo Checking Spark cluster status...
docker ps | findstr pipeline-spark-master >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Spark Master not running!
    echo Start the environment first: start-local-environment.bat
    exit /b 1
)

docker ps | findstr pipeline-spark-worker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Spark Workers not running!
    echo Start the environment first: start-local-environment.bat
    exit /b 1
)

echo [OK] Spark cluster is running
echo.

REM Check if JAR exists
set JAR_FILE=pipeline-examples\target\pipeline-examples-1.0-SNAPSHOT.jar
if not exist "%JAR_FILE%" (
    echo Building project...
    mvn clean package -DskipTests -q
    echo [OK] Build complete
) else (
    echo [OK] JAR file found: %JAR_FILE%
)
echo.

REM Submit to cluster
echo Submitting job to Spark cluster...
echo Pipeline Class: %FULL_CLASS_NAME%
echo Spark Master:   spark://localhost:7077
echo.

docker exec pipeline-spark-master spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode client ^
    --class "%FULL_CLASS_NAME%" ^
    --driver-memory 2g ^
    --executor-memory 2g ^
    --executor-cores 2 ^
    --total-executor-cores 4 ^
    --conf spark.sql.adaptive.enabled=true ^
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 ^
    /opt/spark-apps/pipeline-examples-1.0-SNAPSHOT.jar

set EXIT_CODE=%ERRORLEVEL%

echo.
echo ================================================================================

if %EXIT_CODE% equ 0 (
    echo [OK] Pipeline completed successfully on Spark cluster!
    echo.
    echo View cluster details:
    echo   Spark Master UI:    http://localhost:8081
    echo   Worker 1 UI:        http://localhost:8082
    echo   Worker 2 UI:        http://localhost:8083
    echo   History Server:     http://localhost:18080
) else (
    echo [ERROR] Pipeline failed with exit code: %EXIT_CODE%
    echo.
    echo Troubleshooting:
    echo   1. Check Spark Master logs:  docker logs pipeline-spark-master
    echo   2. Check Worker logs:         docker logs pipeline-spark-worker-1
    echo   3. View Spark UI:             http://localhost:8081
)

echo ================================================================================

exit /b %EXIT_CODE%
