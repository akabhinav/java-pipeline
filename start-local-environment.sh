#!/bin/bash

################################################################################
# Enterprise Data Pipeline - Complete Local Testing Environment
#
# This script starts all services and initializes the environment:
# - Docker Compose (Kafka, MinIO, PostgreSQL)
# - Sample data upload
# - Kafka topics creation
# - Pipeline execution
#
# Usage:
#   ./start-local-environment.sh               # Start everything
#   ./start-local-environment.sh --stop        # Stop all services
#   ./start-local-environment.sh --restart     # Restart all services
#   ./start-local-environment.sh --status      # Check status
#   ./start-local-environment.sh --logs        # View logs
################################################################################

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Default action
ACTION="start"

# Parse arguments
if [ "$1" == "--stop" ]; then
    ACTION="stop"
elif [ "$1" == "--restart" ]; then
    ACTION="restart"
elif [ "$1" == "--status" ]; then
    ACTION="status"
elif [ "$1" == "--logs" ]; then
    ACTION="logs"
elif [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    grep '^#' "$0" | grep -v '#!/bin/bash' | sed 's/^# //'
    exit 0
fi

print_header() {
    echo ""
    echo -e "${BLUE}================================================================================${NC}"
    echo -e "${BLUE}  $1${NC}"
    echo -e "${BLUE}================================================================================${NC}"
    echo ""
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    echo -e "${YELLOW}Checking prerequisites...${NC}"

    if ! command -v docker &> /dev/null; then
        print_error "Docker not found. Please install Docker first."
        exit 1
    fi
    print_success "Docker installed"

    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose not found. Please install Docker Compose first."
        exit 1
    fi
    print_success "Docker Compose installed"

    echo ""
}

# Stop services
stop_services() {
    print_header "Stopping All Services"

    echo -e "${YELLOW}Stopping Docker containers...${NC}"
    docker-compose down

    print_success "All services stopped"
    echo ""
}

# Check service status
check_status() {
    print_header "Service Status"

    docker-compose ps

    echo ""
    print_info "Service URLs:"
    echo "  Kafka UI:       http://localhost:8080"
    echo "  MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)"
    echo "  pgAdmin:        http://localhost:5050  (admin@pipeline.com/admin123)"
    echo "  PostgreSQL:     localhost:5432         (pipeline/pipeline123)"
    echo "  Kafka:          localhost:9092"
    echo ""
}

# View logs
view_logs() {
    print_header "Service Logs (Ctrl+C to exit)"
    docker-compose logs -f --tail=100
}

# Start services
start_services() {
    print_header "Enterprise Data Pipeline - Local Testing Environment"

    check_prerequisites

    # Step 1: Start Docker Compose
    print_info "Step 1: Starting Docker containers..."
    echo -e "${YELLOW}Services: Kafka, Zookeeper, MinIO, PostgreSQL, pgAdmin, Kafka UI${NC}"
    echo ""

    docker-compose up -d

    print_success "Docker containers started"
    echo ""

    # Step 2: Wait for services to be healthy
    print_info "Step 2: Waiting for services to be ready (30 seconds)..."

    SERVICES=("pipeline-zookeeper" "pipeline-kafka" "pipeline-minio" "pipeline-postgres")
    for service in "${SERVICES[@]}"; do
        echo -n "  Waiting for $service..."
        for i in {1..30}; do
            if docker inspect -f '{{.State.Health.Status}}' $service 2>/dev/null | grep -q "healthy"; then
                echo -e " ${GREEN}✓${NC}"
                break
            elif docker inspect -f '{{.State.Status}}' $service 2>/dev/null | grep -q "running"; then
                # For services without healthcheck
                echo -e " ${GREEN}✓${NC}"
                break
            fi
            sleep 1
            echo -n "."
        done
    done

    print_success "All services are ready"
    echo ""

    # Step 3: Initialize services (upload data, create topics)
    print_info "Step 3: Initializing services..."
    echo ""

    chmod +x docker/init-services.sh
    ./docker/init-services.sh

    echo ""

    # Step 4: Display service information
    print_header "✅ Environment Ready!"

    echo -e "${GREEN}All services are running!${NC}"
    echo ""

    echo -e "${CYAN}📊 Service URLs:${NC}"
    echo -e "  ${YELLOW}Kafka UI:${NC}       http://localhost:8080"
    echo -e "  ${YELLOW}MinIO Console:${NC}  http://localhost:9001  (minioadmin/minioadmin)"
    echo -e "  ${YELLOW}pgAdmin:${NC}        http://localhost:5050  (admin@pipeline.com/admin123)"
    echo -e "  ${YELLOW}PostgreSQL:${NC}     localhost:5432         (pipeline/pipeline123)"
    echo -e "  ${YELLOW}Kafka:${NC}          localhost:9092"
    echo ""

    echo -e "${CYAN}💾 Sample Data Loaded:${NC}"
    echo "  ✓ PostgreSQL: 15 customers, 15 loan applications, 30 transactions"
    echo "  ✓ MinIO (S3): customers.csv, credit_bureau.csv"
    echo "  ✓ Kafka: 4 topics created (loan-applications, transactions, etc.)"
    echo ""

    echo -e "${CYAN}🚀 Next Steps:${NC}"
    echo "  1. View services:        docker-compose ps"
    echo "  2. View logs:            ./start-local-environment.sh --logs"
    echo "  3. Run pipeline:         cd pipeline-examples && mvn exec:java -Dexec.mainClass=\"...\""
    echo "  4. Stop services:        ./start-local-environment.sh --stop"
    echo ""

    echo -e "${CYAN}📚 Access Data:${NC}"
    echo "  • PostgreSQL (psql):     psql -h localhost -U pipeline -d banking"
    echo "  • MinIO Browser:         http://localhost:9001"
    echo "  • Kafka Topics:          docker exec -it pipeline-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic loan-applications --from-beginning"
    echo ""

    print_header "Environment is Ready for Testing!"
}

# Main execution
case $ACTION in
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        stop_services
        sleep 2
        start_services
        ;;
    status)
        check_status
        ;;
    logs)
        view_logs
        ;;
    *)
        echo "Unknown action: $ACTION"
        echo "Use --help for usage information"
        exit 1
        ;;
esac
