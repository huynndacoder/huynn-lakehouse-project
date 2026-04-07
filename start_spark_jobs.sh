#!/bin/bash
set -e

echo "=========================================="
echo "NYC Taxi Analytics - Spark Job Starter"
echo "=========================================="

CHECKPOINT_BACKUP_INTERVAL=300

backup_checkpoints() {
    local source=$1
    local backup=$2
    local name=$3
    
    if mc ls datalake/$(echo "$source" | sed 's|s3a://datalake/||') >/dev/null 2>&1; then
        mc cp --recursive "datalake/$(echo "$source" | sed 's|s3a://datalake/||')" "datalake/$(echo "$backup" | sed 's|s3a://datalake/||')" 2>/dev/null || \
        echo "  [WARN] Could not backup checkpoints for $name"
    else
        echo "  [WARN] Source checkpoint path not found: $source"
    fi
}

restore_checkpoints() {
    local backup=$1
    local restore=$2
    local name=$3
    
    if mc ls datalake/$(echo "$backup" | sed 's|s3a://datalake/||') >/dev/null 2>&1; then
        mc cp --recursive "datalake/$(echo "$backup" | sed 's|s3a://datalake/||')" "datalake/$(echo "$restore" | sed 's|s3a://datalake/||')" 2>/dev/null && \
        echo "  [RESTORE] Checkpoints restored for $name from backup" || \
        echo "  [ERROR] Failed to restore checkpoints for $name"
    else
        echo "  [ERROR] No backup found for $name"
        return 1
    fi
}

start_checkpoint_backup() {
    local primary=$1
    local backup=$2
    local name=$3
    
    (
        while true; do
            sleep $CHECKPOINT_BACKUP_INTERVAL
            backup_checkpoints "$primary" "$backup" "$name"
            echo "  [BACKUP] Checkpoints synced for $name at $(date)"
        done
    ) &
    echo "  Started checkpoint backup for $name in background"
}

restore_checkpoints_if_needed() {
    local primary=$1
    local backup=$2
    local name=$3
    
    if ! mc ls "datalake/$(echo "$primary" | sed 's|s3a://datalake/||')" >/dev/null 2>&1 && \
       mc ls "datalake/$(echo "$backup" | sed 's|s3a://datalake/||')" >/dev/null 2>&1; then
        echo "  [RECOVERY] Primary checkpoint missing, restoring from backup: $name"
        restore_checkpoints "$backup" "$primary" "$name"
    fi
}

wait_for_service() {
    local service=$1
    local host=$2
    local port=$3
    local max_attempts=${4:-60}
    local attempt=1
    
    echo "Waiting for $service ($host:$port)..."
    
    # Try multiple methods to check connectivity
    while true; do
        # Method 1: Try using bash /dev/tcp (works in most bash environments)
        if bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null; then
            echo "  $service is ready!"
            return 0
        fi
        
        # Method 2: Try curl for HTTP-based services
        if [ "$port" = "9000" ] || [ "$port" = "9001" ]; then
            if curl -s -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        # Method 3: Try docker exec for Kafka
        if [ "$service" = "Kafka" ]; then
            if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        # Method 4: Try docker exec for MinIO
        if [ "$service" = "MinIO" ]; then
            if docker exec minio mc ready local 2>/dev/null; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        # Method 5: Try docker exec for Spark Master
        if [[ "$service" == *"Spark"* ]]; then
            if docker exec spark-master curl -s -o /dev/null http://localhost:7077 2>/dev/null; then
                echo "  $service is ready!"
                return 0
            fi
            # Also try checking via docker health
            if docker inspect --format='{{.State.Health.Status}}' spark-master 2>/dev/null | grep -q "healthy"; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        if [ $attempt -ge $max_attempts ]; then
            echo "ERROR: $service not available after $max_attempts seconds"
            return 1
        fi
        echo "  Attempt $attempt/$max_attempts - $service not ready, waiting..."
        sleep 2
        attempt=$((attempt + 1))
    done
}

echo ""
echo "Step 1: Waiting for core services..."

wait_for_service "Kafka" "kafka" 9092 || exit 1
wait_for_service "MinIO" "minio" 9000 || exit 1
wait_for_service "Spark Master" "spark-master" 7077 || exit 1
wait_for_service "Spark Web UI" "spark-master" 8080 || exit 1

echo ""
echo "Step 2: Checking Kafka topics..."
sleep 5

kafka_topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "")
echo "Available topics: $kafka_topics"

if [[ "$kafka_topics" == *"postgres.public.nyc_taxi_trips"* ]]; then
    echo "  [OK] nyc_taxi_trips topic exists"
else
    echo "  [WARN] nyc_taxi_trips topic not found (Debezium may not be configured yet)"
fi

if [[ "$kafka_topics" == *"postgres.public.nyc_weather_hourly"* ]]; then
    echo "  [OK] nyc_weather_hourly topic exists"
else
    echo "  [WARN] nyc_weather_hourly topic not found (Debezium may not be configured yet)"
fi

echo ""
echo "Step 3: Submitting Spark CDC Jobs..."

SPARK_MASTER="spark://spark-master:7077"
SPARK_JOBS_DIR="/spark_jobs"

submit_spark_job() {
    local job_name=$1
    local job_file=$2
    local class_name=$3
    
    echo "  Submitting $job_name..."
    
    docker exec -u spark -d spark-master /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --name "$job_name" \
        --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.cores.max=4 \
        --conf spark.executor.cores=2 \
        --conf spark.default.parallelism=8 \
        --conf spark.driver.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.executor.extraClassPath=/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.lakehouse.type=hadoop \
        --conf spark.sql.catalog.lakehouse.warehouse=s3a://datalake/warehouse \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=admin \
        --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        /spark_jobs/$job_file \
        > /tmp/$job_name.log 2>&1 &
    
    echo "  $job_name submitted in background (logs: /tmp/$job_name.log)"
}

echo ""
echo "Step 4: Submitting Medallion CDC Jobs..."

submit_spark_job "TaxiMedallionCDC" "taxi_cdc.py" "TaxiMedallionCDC"
sleep 2
submit_spark_job "WeatherMedallionCDC" "weather_cdc.py" "WeatherMedallionCDC"

echo ""
echo "Step 4b: Submitting Gold Layer Aggregation Job..."
sleep 5
submit_spark_job "GoldAggregations" "gold_aggregations.py" "GoldAggregations"

echo ""
echo "Step 5: Starting checkpoint backup service..."

TAXI_CHECKPOINT_PRIMARY="s3a://datalake/checkpoints/taxi_prod_cdc_stream"
TAXI_CHECKPOINT_BACKUP="s3a://datalake/checkpoints/taxi_prod_cdc_stream_backup"
WEATHER_CHECKPOINT_PRIMARY="s3a://datalake/checkpoints/weather_v4"
WEATHER_CHECKPOINT_BACKUP="s3a://datalake/checkpoints/weather_v4_backup"

echo "  Checkpoint backup interval: ${CHECKPOINT_BACKUP_INTERVAL}s"
echo "  Taxi: $TAXI_CHECKPOINT_PRIMARY -> $TAXI_CHECKPOINT_BACKUP"
echo "  Weather: $WEATHER_CHECKPOINT_PRIMARY -> $WEATHER_CHECKPOINT_BACKUP"

echo ""
echo "  Checking for checkpoint recovery..."
restore_checkpoints_if_needed "$TAXI_CHECKPOINT_PRIMARY" "$TAXI_CHECKPOINT_BACKUP" "taxi_cdc"
restore_checkpoints_if_needed "$WEATHER_CHECKPOINT_PRIMARY" "$WEATHER_CHECKPOINT_BACKUP" "weather_cdc"

start_checkpoint_backup "$TAXI_CHECKPOINT_PRIMARY" "$TAXI_CHECKPOINT_BACKUP" "taxi_cdc"
start_checkpoint_backup "$WEATHER_CHECKPOINT_PRIMARY" "$WEATHER_CHECKPOINT_BACKUP" "weather_cdc"

echo ""
echo "=========================================="
echo "Spark Jobs Submitted Successfully!"
echo "=========================================="
echo ""
echo "Monitor jobs at:"
echo "  - Spark Master UI: http://localhost:8090"
echo ""
echo "To check running jobs:"
echo "  docker exec spark-master /opt/spark/bin/spark-submit --list"
echo ""
echo "To stop all jobs, restart containers or kill spark processes."
