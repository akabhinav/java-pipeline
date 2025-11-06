@echo off
REM ============================================================================
REM Run Data Lake Pipeline (Windows)
REM
REM This script runs the Data Lake pipeline demonstrating:
REM - Medallion Architecture (Bronze/Silver/Gold)
REM - Delta Lake features (ACID, Time Travel, Schema Evolution)
REM - MinIO as S3-compatible storage
REM
REM Prerequisites:
REM   - Local environment running: start-local-environment.bat
REM   - MinIO available at: http://localhost:9000
REM
REM Usage:
REM   run-data-lake.bat
REM ============================================================================

setlocal enabledelayedexpansion

echo ================================================================================
echo   Data Lake Pipeline - Medallion Architecture
echo ================================================================================
echo.

REM Check if MinIO is running
echo Checking prerequisites...
docker ps | findstr pipeline-minio >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] MinIO not running!
    echo Start the environment first: start-local-environment.bat
    exit /b 1
)

echo [OK] MinIO is running
echo.

REM Show what will happen
echo This pipeline will:
echo   1. Write raw data to Bronze Layer (s3a://data-lake/bronze/)
echo   2. Clean and validate to Silver Layer (s3a://data-lake/silver/)
echo   3. Aggregate to Gold Layer (s3a://data-lake/gold/)
echo   4. Demonstrate Delta Lake features:
echo      - ACID transactions
echo      - Time travel
echo      - Upserts (merge)
echo      - Schema evolution
echo.

echo Running Data Lake pipeline...
echo.

cd pipeline-examples

mvn exec:java ^
    -Dexec.mainClass="com.enterprise.pipeline.examples.DataLakePipeline" ^
    -Dexec.cleanupDaemonThreads=false

set EXIT_CODE=%ERRORLEVEL%

echo.
echo ================================================================================

if %EXIT_CODE% equ 0 (
    echo [OK] Data Lake Pipeline completed successfully!
    echo.
    echo View your Data Lake:
    echo   MinIO Console:  http://localhost:9001
    echo   Bucket:         data-lake
    echo   Layers:         bronze/, silver/, gold/
    echo.
    echo Query your data:
    echo   spark.read^(^).format^("delta"^).load^("s3a://data-lake/gold/loan_summary_by_purpose"^)
    echo.
    echo Read DATA_LAKE_GUIDE.md for comprehensive documentation
) else (
    echo [ERROR] Pipeline failed with exit code: %EXIT_CODE%
    echo.
    echo Troubleshooting:
    echo   1. Check MinIO is running:     docker ps ^| findstr minio
    echo   2. Verify data-lake bucket:    http://localhost:9001
    echo   3. Check logs above for errors
)

echo ================================================================================

exit /b %EXIT_CODE%
