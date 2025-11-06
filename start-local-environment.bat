@echo off
REM ============================================================================
REM Enterprise Data Pipeline - Complete Local Testing Environment (Windows)
REM
REM This script starts all services and initializes the environment
REM ============================================================================

setlocal enabledelayedexpansion

set ACTION=start

if "%1"=="--stop" set ACTION=stop
if "%1"=="--restart" set ACTION=restart
if "%1"=="--status" set ACTION=status
if "%1"=="--logs" set ACTION=logs
if "%1"=="--help" goto :help
if "%1"=="-h" goto :help

goto :%ACTION%

:help
echo.
echo Usage:
echo   start-local-environment.bat               Start everything
echo   start-local-environment.bat --stop        Stop all services
echo   start-local-environment.bat --restart     Restart all services
echo   start-local-environment.bat --status      Check status
echo   start-local-environment.bat --logs        View logs
echo.
goto :end

:start
echo ================================================================================
echo   Enterprise Data Pipeline - Local Testing Environment
echo ================================================================================
echo.

REM Check Docker
where docker >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Docker not found. Please install Docker Desktop for Windows.
    goto :end
)
echo [OK] Docker installed

REM Check Docker Compose
where docker-compose >nul 2>nul
if %ERRORLEVEL% neq 0 (
    docker compose version >nul 2>nul
    if %ERRORLEVEL% neq 0 (
        echo [ERROR] Docker Compose not found.
        goto :end
    )
)
echo [OK] Docker Compose installed
echo.

REM Start services
echo Starting Docker containers...
docker-compose up -d
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to start services
    goto :end
)

echo Waiting for services to be ready (30 seconds)...
timeout /t 30 /nobreak >nul

REM Initialize services
echo.
echo Initializing services...
call docker\init-services.bat

echo.
echo ================================================================================
echo   Environment Ready!
echo ================================================================================
echo.
echo Service URLs:
echo   Kafka UI:       http://localhost:8080
echo   MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)
echo   pgAdmin:        http://localhost:5050  (admin@pipeline.com/admin123)
echo   PostgreSQL:     localhost:5432         (pipeline/pipeline123)
echo.
echo Next Steps:
echo   1. View services:  docker-compose ps
echo   2. View logs:      start-local-environment.bat --logs
echo   3. Run pipeline:   cd pipeline-examples ^&^& mvn exec:java -Dexec.mainClass="com.enterprise.pipeline.examples.RealConnectorsPipeline"
echo   4. Stop services:  start-local-environment.bat --stop
echo.
goto :end

:stop
echo Stopping all services...
docker-compose down
echo Services stopped.
goto :end

:restart
call :stop
timeout /t 2 /nobreak >nul
call :start
goto :end

:status
echo Service Status:
docker-compose ps
echo.
echo Service URLs:
echo   Kafka UI:       http://localhost:8080
echo   MinIO Console:  http://localhost:9001
echo   pgAdmin:        http://localhost:5050
echo   PostgreSQL:     localhost:5432
goto :end

:logs
echo Viewing logs (Ctrl+C to exit)...
docker-compose logs -f --tail=100
goto :end

:end
endlocal
