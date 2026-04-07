#!/bin/bash
# fix_pipeline.sh - Initialize and fix the CDC pipeline
# Usage: ./scripts/fix_pipeline.sh

set -e

echo "=========================================="
echo "Pipeline Health Check and Fix"
echo "=========================================="
echo ""

# Function to check if service is healthy
check_service() {
    local service=$1
    local max_attempts=30
    local attempt=1
    
    echo "Checking $service..."
    while [ $attempt -le $max_attempts ]; do
        if docker compose ps "$service" 2>/dev/null | grep -q "healthy\|running"; then
            echo "  ✓ $service is running"
            return 0
        fi
        echo "  Waiting for $service... (attempt $attempt/$max_attempts)"
        sleep 2
        attempt=$((attempt + 1))
    done
    echo "  ✗ $service failed to start"
    return 1
}

# Function to register Debezium connector
register_debezium() {
    echo ""
    echo "Registering Debezium connector..."
    
    # Check if already registered
    if curl -s http://localhost:8083/connectors/postgres-taxi-connector/status 2>/dev/null | grep -q "RUNNING"; then
        echo "  ✓ Debezium connector already running"
        return 0
    fi
    
    # Try to register
    local max_attempts=10
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -X POST http://localhost:8083/connectors \
            -H "Content-Type: application/json" \
            -d @/home/huynnz/GetAJob/Huynz/debezium.json 2>/dev/null | grep -q "name"; then
            echo "  ✓ Debezium connector registered"
            sleep 5
            if curl -s http://localhost:8083/connectors/postgres-taxi-connector/status 2>/dev/null | grep -q "RUNNING"; then
                echo "  ✓ Debezium connector is running"
                return 0
            fi
        fi
        echo "  Attempt $attempt/$max_attempts - waiting for Debezium..."
        sleep 3
        attempt=$((attempt + 1))
    done
    
    echo "  ✗ Failed to register Debezium connector"
    echo "  Run manually: ./huynn.sh debezium:register"
    return 1
}

# Function to wait for Kafka topics
wait_for_topics() {
    echo ""
    echo "Waiting for Kafka topics..."
    sleep 10
    
    local topics
    topics=$(docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | grep "postgres.public" || true)
    
    if [ -n "$topics" ]; then
        echo "  ✓ CDC topics created:"
        echo "$topics" | while read line; do
            echo "    - $line"
        done
        return 0
    else
        echo "  ✗ No CDC topics found. Debezium may not be running."
        echo "  Run: ./huynn.sh debezium:register"
        return 1
    fi
}

# Function to unpuase Airflow DAGs
unpause_dags() {
    echo ""
    echo "Unpausing Airflow DAGs..."
    
    docker exec airflow airflow dags unpause taxi_parquet_to_postgres 2>/dev/null || echo "  [WARN] taxi_parquet_to_postgres already unpaused"
    docker exec airflow airflow dags unpause weather_to_postgres 2>/dev/null || echo "  [WARN] weather_to_postgres already unpaused"
    docker exec airflow airflow dags unpause spark_cdc_medallion_pipeline 2>/dev/null || echo "  [WARN] spark_cdc_medallion_pipeline already unpaused"
    
    echo "  ✓ DAGs unpaused"
}

# Function to check Spark applications
check_spark() {
    echo ""
    echo "Checking Spark applications..."
    
    local apps
    apps=$(curl -s http://localhost:8090/api/v1/applications 2>/dev/null | grep -o '"name":"[^"]*"' | grep -c "TaxiMedallionCDC" || echo "0")
    
    if [ "$apps" -gt 0 ]; then
        echo "  ✓ TaxiMedallionCDC Spark job is running"
        return 0
    else
        echo "  [WARN] No Spark applications found"
        echo "  Run: ./huynn.sh spark:jobs"
        return 1
    fi
}

# Function to show summary
show_summary() {
    echo ""
    echo "=========================================="
    echo "Pipeline Summary"
    echo "=========================================="
    echo ""
    
    echo "PostgreSQL (Source):"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips FROM yellow_taxi_trips;" 2>/dev/null || echo "  ✗ Not accessible"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as weather FROM nyc_weather_hourly;" 2>/dev/null || echo "  ✗ Weather data missing"
    docker exec postgres psql -U admin -d weather -c "SELECT MIN(tpep_pickup_datetime) as min_date, MAX(tpep_pickup_datetime) as max_date FROM yellow_taxi_trips;" 2>/dev/null || true
    echo ""
    
    echo "Debezium:"
    local state
    state=$(curl -s http://localhost:8083/connectors/postgres-taxi-connector/status 2>/dev/null | jq -r '.connector.state' 2>/dev/null || echo "Not registered")
    echo "  State: $state"
    echo ""
    
    echo "Kafka Topics:"
    docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | grep "postgres.public" || echo "  ✗ No CDC topics"
    echo ""
    
    echo "ClickHouse (Real-time):"
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT COUNT(*) as count FROM lakehouse.taxi_prod" 2>/dev/null || echo "  ✗ Not accessible"
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT MIN(tpep_pickup_datetime) as min_time, MAX(tpep_pickup_datetime) as max_time FROM lakehouse.taxi_prod" 2>/dev/null || true
    echo ""
    
    echo "Weather Simulator:"
    docker logs weather-simulator --tail 3 2>/dev/null | grep -E "Weather updated|Connected" || echo "  [WARN] Not running or starting"
    echo ""
    
    echo "API Health:"
    curl -s http://localhost:8001/health 2>/dev/null | jq -r '.message' || echo "  ✗ API not accessible"
    echo ""
    
    echo "=========================================="
    echo "Next Steps:"
    echo "=========================================="
    echo ""
    echo "1. Start Spark jobs (if not running):"
    echo "   ./huynn.sh spark:jobs"
    echo ""
    echo "2. Backfill weather data (if needed):"
    echo "   ./huynn.sh weather:backfill"
    echo ""
    echo "3. Access dashboard:"
    echo "   ./huynn.sh api:health"
    echo "   Open: http://localhost:8505"
    echo ""
    echo "4. Check pipeline status:"
    echo "   ./huynn.sh pipeline:status"
    echo ""
}

# Main execution
echo "Step 1: Checking services..."
echo ""

check_service "postgres" || true
check_service "kafka" || true
check_service "clickhouse" || true
check_service "debezium" || true

echo ""
echo "Step 2: Registering Debezium..."
register_debezium || true

echo ""
echo "Step 3: Waiting for Kafka topics..."
wait_for_topics || true

echo ""
echo "Step 4: Unpausing Airflow DAGs..."
unpause_dags || true

echo ""
echo "Step 5: Checking Spark applications..."
check_spark || true

echo ""
show_summary