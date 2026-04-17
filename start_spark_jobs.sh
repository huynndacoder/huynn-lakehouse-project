#!/bin/bash
set -e

cleanup_on_exit() {
    echo "Stopping background backup processes..."
    if ls /tmp/spark_checkpoint_pids/*.pid 1> /dev/null 2>&1; then
        kill $(cat /tmp/spark_checkpoint_pids/*.pid) 2>/dev/null || true
        rm -f /tmp/spark_checkpoint_pids/*.pid
    fi
}
trap cleanup_on_exit EXIT INT TERM

echo "=========================================="
echo "NYC Taxi Analytics - Spark Job Starter"
echo "=========================================="

CHECKPOINT_BACKUP_INTERVAL=300
CHECKPOINT_PID_DIR="/tmp/spark_checkpoint_pids"

cleanup_orphaned_process() {
    local name=$1
    local pidfile="$CHECKPOINT_PID_DIR/${name}.pid"
    
    if [ -f "$pidfile" ]; then
        local old_pid=$(cat "$pidfile" 2>/dev/null)
        if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
            echo "  [CLEANUP] Stopping existing backup process (PID: $old_pid) for $name"
            kill "$old_pid" 2>/dev/null || true
            sleep 1
        fi
        rm -f "$pidfile"
    fi
}

is_backup_running() {
    local name=$1
    local pidfile="$CHECKPOINT_PID_DIR/${name}.pid"
    
    if [ -f "$pidfile" ]; then
        local pid=$(cat "$pidfile" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        rm -f "$pidfile"
    fi
    return 1
}

backup_checkpoints() {
    local source=$1
    local backup=$2
    local name=$3
    
    if mc ls datalake/$(echo "$source" | sed 's|s3a://datalake/||') >/dev/null 2>&1; then
        mc cp --recursive "datalake/$(echo "$source" | sed 's|s3a://datalake/||')" "datalake/$(echo "$backup" | sed 's|s3a://datalake/||')" 2>/dev/null || \
        echo "  [WARN] Could not backup checkpoints for $name"
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
    local pidfile="$CHECKPOINT_PID_DIR/${name}.pid"
    
    mkdir -p "$CHECKPOINT_PID_DIR"
    
    if is_backup_running "$name"; then
        echo "  [SKIP] Backup for $name is already running (PID: $(cat "$pidfile"))"
        return 0
    fi
    
    cleanup_orphaned_process "$name"
    
    (
        while true; do
            sleep $CHECKPOINT_BACKUP_INTERVAL
            backup_checkpoints "$primary" "$backup" "$name"
        done
    ) &
    
    echo "$!" > "$pidfile"
    echo "  Started checkpoint backup for $name (PID: $!)"
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
    
    while true; do
        if bash -c "echo >/dev/tcp/$host/$port" 2>/dev/null; then
            echo "  $service is ready!"
            return 0
        fi
        
        if [ "$port" = "9000" ] || [ "$port" = "9001" ]; then
            if curl -s -o /dev/null http://localhost:9000/minio/health/live 2>/dev/null; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        if [ "$service" = "Kafka" ]; then
            if docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        if [ "$service" = "MinIO" ]; then
            if docker exec minio mc ready local 2>/dev/null; then
                echo "  $service is ready!"
                return 0
            fi
        fi
        
        if [[ "$service" == *"Spark"* ]]; then
            if docker exec spark-master curl -s -o /dev/null http://localhost:7077 2>/dev/null; then
                echo "  $service is ready!"
                return 0
            fi
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

submit_spark_job() {
    local job_name=$1
    local job_file=$2
    local SPARK_MASTER="spark://spark-master:7077"
    
    echo "  -> Dispatching $job_name..."
    
    #Please change spark cores and default parallelism If your machine has more resources than mine.
    docker exec -u spark spark-master /opt/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        --deploy-mode client \
        --name "$job_name" \
        --jars /opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
        --conf spark.cores.max=1 \
        --conf spark.executor.cores=1 \
        --conf spark.default.parallelism=4 \
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
}

# ==========================================
# EXECUTION FLOW
# ==========================================

echo ""
echo "Step 1: Waiting for core services..."
wait_for_service "Kafka" "kafka" 9092 || exit 1
wait_for_service "MinIO" "minio" 9000 || exit 1
wait_for_service "Spark Master" "spark-master" 7077 || exit 1
wait_for_service "Spark Web UI" "spark-master" 8080 || exit 1

echo ""
echo "Step 2: Checking Kafka topics..."
sleep 2
kafka_topics=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null || echo "")
[[ "$kafka_topics" == *"postgres.public.yellow_taxi_trips"* ]] && echo "  [OK] yellow_taxi_trips" || echo "  [WARN] yellow_taxi_trips not found"
[[ "$kafka_topics" == *"postgres.public.nyc_weather_hourly"* ]] && echo "  [OK] nyc_weather_hourly" || echo "  [WARN] nyc_weather_hourly not found"

echo ""
echo "Step 3: Dispatching Taxi Zones (Async)..."
docker cp ./data/taxi_zone_lookup.csv spark-master:/tmp/taxi_zone_lookup.csv 2>/dev/null || true
docker cp ./data/taxi_zone_lookup.csv spark-worker:/tmp/taxi_zone_lookup.csv 2>/dev/null || true
submit_spark_job "LoadTaxiZones" "load_zones.py"
echo "  [OK] Load job running in background (Python will handle exists checks)."

echo ""
echo "Step 4: Analyzing Spark Job Topology..."
# Fetch ONLY currently running applications using the Spark REST API
running_jobs=$(docker exec spark-master curl -sf "http://localhost:8080/api/v1/applications?status=running" 2>/dev/null || echo "")

# Define the expected jobs and their files
JOB_NAMES=("TaxiMedallionCDC" "WeatherMedallionCDC" "GoldAggregations")
JOB_FILES=("taxi_cdc.py" "weather_cdc.py" "gold_aggregations.py")

JOBS_TO_RUN=()
FILES_TO_RUN=()

echo "------------------------------------------------------------------"
printf "%-25s | %-12s | %s\n" "JOB NAME" "STATE" "ACTION"
echo "------------------------------------------------------------------"

for i in "${!JOB_NAMES[@]}"; do
    name="${JOB_NAMES[$i]}"
    file="${JOB_FILES[$i]}"

    # Simple, bulletproof grep instead of fragile text slicing
    if echo "$running_jobs" | grep -q "$name"; then
        printf "%-25s | %-12s | %s\n" "$name" "RUNNING" "SKIPPING"
    else
        printf "%-25s | %-12s | %s\n" "$name" "OFFLINE" "QUEUED FOR SUBMISSION"
        JOBS_TO_RUN+=("$name")
        FILES_TO_RUN+=("$file")
    fi
done
echo "------------------------------------------------------------------"

if [ ${#JOBS_TO_RUN[@]} -eq 0 ]; then
    echo ""
    echo "  All data pipelines are already active! Nothing to submit."
else
    echo ""
    echo "Step 5: Submitting Offline Jobs..."
    for i in "${!JOBS_TO_RUN[@]}"; do
        submit_spark_job "${JOBS_TO_RUN[$i]}" "${FILES_TO_RUN[$i]}"
        sleep 5 # Stagger submissions to avoid hammering the master node
    done
fi

echo ""
echo "Step 6: Starting checkpoint backup service..."
TAXI_CHECKPOINT_PRIMARY="s3a://datalake/checkpoints/taxi_prod_cdc_stream"
TAXI_CHECKPOINT_BACKUP="s3a://datalake/checkpoints/taxi_prod_cdc_stream_backup"
WEATHER_CHECKPOINT_PRIMARY="s3a://datalake/checkpoints/weather_v4"
WEATHER_CHECKPOINT_BACKUP="s3a://datalake/checkpoints/weather_v4_backup"

restore_checkpoints_if_needed "$TAXI_CHECKPOINT_PRIMARY" "$TAXI_CHECKPOINT_BACKUP" "taxi_cdc"
restore_checkpoints_if_needed "$WEATHER_CHECKPOINT_PRIMARY" "$WEATHER_CHECKPOINT_BACKUP" "weather_cdc"

start_checkpoint_backup "$TAXI_CHECKPOINT_PRIMARY" "$TAXI_CHECKPOINT_BACKUP" "taxi_cdc"
start_checkpoint_backup "$WEATHER_CHECKPOINT_PRIMARY" "$WEATHER_CHECKPOINT_BACKUP" "weather_cdc"

echo ""
echo "=========================================="
echo "Spark Execution Plan Completed!"
echo "Monitor jobs at: http://localhost:8090"
echo "=========================================="