#!/bin/bash
# huynn.sh — Helper CLI for NYC Taxi Analytics Pipeline
# Usage: ./huynn.sh <command> [args]

set -e

CMD=$1
shift

case "$CMD" in

  # ── Quick Start ──────────────────────────────────────────────
  quickstart)
    echo "=== Quick Start ==="
    echo ""
    echo "[1/5] Starting services..."
    cd /home/huynnz/GetAJob/Huynz && docker compose up -d
    echo "[2/5] Waiting for services (30s)..."
    sleep 30
    echo "[3/5] Initializing CDC pipeline..."
    ./scripts/fix_pipeline.sh
    echo "[4/5] Starting Spark jobs..."
    ./start_spark_jobs.sh
    echo "[5/5] Verifying data..."
    echo "PostgreSQL:"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips FROM yellow_taxi_trips;" 2>/dev/null || echo "  Not accessible"
    echo ""
    echo "Iceberg (via Doris):"
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) FROM iceberg_hadoop.lakehouse.silver_taxi_trips" 2>/dev/null | grep -v "cnt" | head -1 || echo "  Not accessible"
    echo ""
    echo "=== Quick Start Complete ==="
    echo ""
    echo "Access:"
    echo "  Dashboard: http://localhost:8505"
    echo "  API Docs:  http://localhost:8001/docs"
    echo "  Spark UI:  http://localhost:8090"
    echo "  Check pipeline: ./huynn.sh pipeline:status"
    ;;

  # ── Stack Management ────────────────────────────────────────
  up)
    cd /home/huynnz/GetAJob/Huynz && docker compose up -d "$@" ;;
  down)
    cd /home/huynnz/GetAJob/Huynz && docker compose down "$@" ;;
  down-v)
    echo "Removing all volumes and data..."
    cd /home/huynnz/GetAJob/Huynz && docker compose down -v "$@" ;;
  restart)
    cd /home/huynnz/GetAJob/Huynz && docker compose restart "$@" ;;
  ps)
    cd /home/huynnz/GetAJob/Huynz && docker compose ps ;;
  logs)
    cd /home/huynnz/GetAJob/Huynz && docker compose logs -f "$@" ;;
  status)
    cd /home/huynnz/GetAJob/Huynz && docker compose ps ;;
  clean)
    echo "Removing containers, volumes, and images..."
    cd /home/huynnz/GetAJob/Huynz && docker compose down -v --rmi all 2>/dev/null || true
    docker system prune -af --volumes 2>/dev/null || true
    echo "Done. Run './huynn.sh up' to start fresh." ;;

  # ── Airflow ──────────────────────────────────────────────
  airflow:sh)
    docker exec -it airflow bash "$@" ;;
  airflow:logs)
    docker logs -f airflow "$@" ;;
  airflow:restart)
    cd /home/huynnz/GetAJob/Huynz && docker compose restart airflow ;;
  airflow:dags)
    docker exec airflow airflow dags list ;;
  airflow:trigger)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh airflow:trigger <dag_id>"
      exit 1
    fi
    docker exec airflow airflow dags trigger "$1" ;;
  airflow:unpause)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh airflow:unpause <dag_id>"
      exit 1
    fi
    docker exec airflow airflow dags unpause "$1" ;;
  airflow:unpause-all)
    echo "Unpausing all DAGs..."
    docker exec airflow airflow dags unpause taxi_parquet_to_postgres 2>/dev/null || true
    docker exec airflow airflow dags unpause spark_cdc_medallion_pipeline 2>/dev/null || true
    echo "Done." ;;

  # ── Debezium ─────────────────────────────────────────────
  debezium:register)
    curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @/home/huynnz/GetAJob/Huynz/debezium.json | python3 -m json.tool ;;
  debezium:status)
    curl -s http://localhost:8083/connectors/postgres-taxi-connector/status | python3 -m json.tool ;;
  debezium:delete)
    curl -s -X DELETE http://localhost:8083/connectors/postgres-taxi-connector | python3 -m json.tool ;;
  debezium:list)
    curl -s http://localhost:8083/connectors | python3 -m json.tool ;;
  debezium:restart)
    curl -s -X POST http://localhost:8083/connectors/postgres-taxi-connector/restart | python3 -m json.tool ;;

  # ── Kafka ────────────────────────────────────────────────
  kafka:topics)
    docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list ;;
  kafka:consume)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:consume <topic>"
      exit 1
    fi
    docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic "$1" --from-beginning ;;
  kafka:consume-live)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:consume-live <topic>"
      exit 1
    fi
    docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic "$1" ;;
  kafka:describe)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:describe <topic>"
      exit 1
    fi
    docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic "$1" ;;
  kafka:count)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:count <topic>"
      exit 1
    fi
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic "$1" --time -1 ;;
  kafka:lag)
    docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group connect-postgres-public-yellow_taxi_trips ;;
  kafka:consumer-groups)
    docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --list ;;

  # ── Spark ────────────────────────────────────────────────
  spark:sh)
    docker exec -it spark-master bash "$@" ;;
  spark:worker:sh)
    docker exec -it spark-worker bash "$@" ;;
  spark:submit)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh spark:submit <job.py>"
      exit 1
    fi
    docker exec -it spark-master /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
      --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
      /spark_jobs/"$1" ;;
  spark:logs)
    docker logs -f spark-master "$@" ;;
  spark:worker:logs)
    docker logs -f spark-worker "$@" ;;
  spark:apps)
    docker exec spark-master curl -sf http://localhost:8080/api/v1/applications 2>/dev/null | python3 -c "import sys,json; apps=json.load(sys.stdin); [print(f'{a[\"name\"]}: {a[\"attempts\"][0][\"completed\"]}') for a in apps]" 2>/dev/null || echo "No apps running" ;;
  spark:jobs)
    ./start_spark_jobs.sh ;;
  spark:clean)
    echo "🧹 Wiping Spark Structured Streaming checkpoints..."
    if [ -d "./minio_data/datalake/checkpoints" ]; then
        sudo rm -rf ./minio_data/datalake/checkpoints/*
        echo "  [OK] Checkpoints cleared! Safe to start jobs."
    else
        echo "  [SKIP] Checkpoint directory not found."
    fi
    ;;

  # ── PostgreSQL ─────────────────────────────────────────────
  postgres:sh)
    docker exec -it postgres psql -U admin -d weather "$@" ;;
  postgres:logs)
    docker logs -f postgres "$@" ;;
  postgres:count)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as total_trips FROM yellow_taxi_trips;" ;;
  postgres:weather)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as total_weather FROM nyc_weather_hourly;" ;;
  postgres:range)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as total, MIN(tpep_pickup_datetime) as min_date, MAX(tpep_pickup_datetime) as max_date FROM yellow_taxi_trips;" ;;
  postgres:recent)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips_last_hour FROM yellow_taxi_trips WHERE tpep_pickup_datetime >= NOW() - INTERVAL '1 hour';" ;;
  postgres:zones)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as total_zones FROM taxi_zones;" ;;

  # ── ClickHouse ─────────────────────────────────────────────
  clickhouse:sh)
    docker exec -it clickhouse clickhouse-client --user admin --password admin ;;
  clickhouse:count)
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT count() as realtime_count FROM lakehouse.taxi_prod" ;;
  clickhouse:tables)
    docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM lakehouse" ;;
  clickhouse:range)
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT count() as count, MIN(tpep_pickup_datetime) as min_time, MAX(tpep_pickup_datetime) as max_time FROM lakehouse.taxi_prod" ;;
  clickhouse:kafka-ingestion)
    docker exec clickhouse clickhouse-client --user admin --password admin --multiquery < /home/huynnz/GetAJob/Huynz/setup_kafka_ingestion.sql ;;
  clickhouse:zones)
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT COUNT(*) FROM lakehouse.taxi_zones" ;;

  # ── Doris / Iceberg ──────────────────────────────────────
  doris:sh)
    docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -u root  ;;
  doris:catalogs)
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root  -e "SHOW CATALOGS;" ;;
  doris:tables)
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root  -e "SHOW CATALOGS;" ;;
  doris:count)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh doris:count <table_name>"
      echo "  Example: ./huynn.sh doris:count silver_taxi_trips"
      echo "  Tables are in: iceberg_hadoop.lakehouse.<table_name>"
      exit 1
    fi
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) as cnt FROM iceberg_hadoop.lakehouse.$1;" ;;
  doris:query)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh doris:query <sql>"
      exit 1
    fi
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "$1" ;;
  doris:switch)
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SWITCH iceberg_hadoop; USE lakehouse; SHOW TABLES;" ;;

  # ── MinIO ────────────────────────────────────────────────
  minio:sh)
    docker exec -it minio bash "$@" ;;
  minio:ls)
    docker exec minio ls -la /data/datalake/warehouse/ ;;
  minio:bucket)
    docker exec minio mc alias set local http://minio:9000 admin admin12345 && \
    docker exec minio mc ls local/datalake/ ;;

  # ── API ──────────────────────────────────────────────────
  api:health)
    curl -s http://localhost:8001/health | python3 -m json.tool ;;
  api:stats)
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/dashboard/stats?mode=realtime" | python3 -m json.tool ;;
  api:stats-historical)
    START_DATE="${1:-2025-01-01}"
    END_DATE="${2:-2025-01-31}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/dashboard/stats?mode=historical&start_date=${START_DATE}&end_date=${END_DATE}" | python3 -m json.tool ;;
  api:zones)
    LIMIT="${1:-10}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/analytics/zones?mode=realtime&limit=${LIMIT}" | python3 -m json.tool ;;
  api:weather)
    HOURS="${1:-24}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/analytics/weather-impact?mode=realtime&hours_back=${HOURS}" | python3 -m json.tool ;;
  api:weather-historical)
    START_DATE="${1:-2025-01-01}"
    END_DATE="${2:-2025-01-31}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/analytics/weather-impact?mode=historical&start_date=${START_DATE}&end_date=${END_DATE}" | python3 -m json.tool ;;
  api:test)
    echo "Testing historical mode (Jan 2025)..."
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/dashboard/stats?mode=historical&start_date=2025-01-01&end_date=2025-01-07" | python3 -m json.tool
    echo ""
    echo "Testing real-time mode..."
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" "http://localhost:8001/api/v1/dashboard/stats?mode=realtime" | python3 -m json.tool ;;

  # ── Dashboard ───────────────────────────────────────────
  dashboard:url)
    echo "Dashboard URL: http://localhost:8505" ;;
  dashboard:logs)
    docker logs -f analytics-dashboard "$@" ;;
  dashboard:restart)
    cd /home/huynnz/GetAJob/Huynz && docker compose restart analytics-dashboard ;;

  # ── Weather ─────────────────────────────────────────────
  weather:status)
    echo "Weather data in PostgreSQL:"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as count, MIN(time) as start_date, MAX(time) as end_date FROM nyc_weather_hourly;" 2>/dev/null || echo "  Not accessible"
    echo ""
    echo "Weather data in Iceberg (via Doris):"
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) as cnt FROM iceberg_hadoop.lakehouse.nyc_weather;" 2>/dev/null || echo "  Not accessible"
    ;;
  weather:count)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as count, MIN(time) as start_date, MAX(time) as end_date FROM nyc_weather_hourly;" ;;
  weather:range)
    docker exec postgres psql -U admin -d weather -c "SELECT MIN(time) as min_time, MAX(time) as max_time, COUNT(*) as records FROM nyc_weather_hourly;" ;;
  weather:logs)
    docker logs -f weather-simulator "$@" ;;

  # ── Pipeline ─────────────────────────────────────────────
  pipeline:status)
    echo "=== Pipeline Health Check ==="
    echo ""
    echo "PostgreSQL (Source):"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips FROM yellow_taxi_trips;" 2>/dev/null || echo "  [ERROR] PostgreSQL not accessible"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as weather FROM nyc_weather_hourly;" 2>/dev/null || true
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as zones FROM taxi_zones;" 2>/dev/null || echo "  [WARN] taxi_zones table empty or missing"
    docker exec postgres psql -U admin -d weather -c "SELECT MIN(tpep_pickup_datetime) as min, MAX(tpep_pickup_datetime) as max FROM yellow_taxi_trips;" 2>/dev/null || true
    echo ""
    echo "Iceberg Catalog (Hadoop):"
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW CATALOGS;" 2>/dev/null || echo "  [ERROR] Doris not accessible"
    echo ""
    echo "Kafka Topics:"
    TOPICS=$(docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | grep "postgres\." || echo "")
    if [ -n "$TOPICS" ]; then
      echo "$TOPICS"
    else
      echo "  [WARN] No CDC topics found - register Debezium: ./huynn.sh debezium:register"
    fi
    echo ""
    echo "Debezium:"
    curl -s http://localhost:8083/connectors/postgres-taxi-connector/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"  State: {d['connector']['state']}\")" 2>/dev/null || echo "  [ERROR] Connector not registered - run: ./huynn.sh debezium:register"
    echo ""
    echo "Iceberg Tables (via Doris):"
    docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SWITCH iceberg_hadoop; USE lakehouse; SHOW TABLES;" 2>/dev/null || echo "  [ERROR] Doris not accessible"
    echo ""
    echo "ClickHouse:"
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT COUNT(*) as count FROM lakehouse.taxi_prod" 2>/dev/null || echo "  [ERROR] ClickHouse not accessible"
    echo ""
    echo "Spark Applications:"
    docker exec spark-master curl -sf http://localhost:8080/api/v1/applications 2>/dev/null | python3 -c "import sys,json; apps=json.load(sys.stdin); [print(f'  {a[\"name\"]}') for a in apps]" 2>/dev/null || echo "  [WARN] No apps running"
    echo ""
    echo "API Health:"
    curl -s http://localhost:8001/health 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  {d[\"message\"]}')" 2>/dev/null || echo "  [ERROR] API not accessible"
    echo ""
    echo "Weather Simulator:"
    docker logs weather-simulator --tail 3 2>/dev/null | grep -E "Weather updated|Connected" || echo "  [WARN] Not running"
    ;;

  pipeline:init)
    echo "=== Initializing Pipeline ==="
    
    echo "0. Seeding taxi_zones into PostgreSQL..."
    
    # Auto-download the CSV if it doesn't exist locally
    if [ ! -f "./data/taxi_zone_lookup.csv" ]; then
        echo "  --> taxi_zone_lookup.csv missing. Downloading from NYC TLC..."
        mkdir -p ./data
        curl -s https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -o ./data/taxi_zone_lookup.csv
    fi
    
    # Create the table
    docker exec -i postgres psql -U admin -d weather -c "
        CREATE TABLE IF NOT EXISTS taxi_zones (
            \"LocationID\" INT PRIMARY KEY,
            \"Borough\" VARCHAR(255),
            \"Zone\" VARCHAR(255),
            \"service_zone\" VARCHAR(255)
        );
        TRUNCATE TABLE taxi_zones;
    "
    # Pipe the local CSV directly into Postgres
    cat ./data/taxi_zone_lookup.csv | docker exec -i postgres psql -U admin -d weather -c "\copy taxi_zones FROM STDIN WITH (FORMAT csv, HEADER true);"

    echo "1. Registering Debezium..."
    curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @/home/huynnz/GetAJob/Huynz/debezium.json | python3 -m json.tool || echo "Debezium already registered or still booting."

    echo "2. Linking Kafka to ClickHouse..."
    docker exec -i clickhouse clickhouse-client --user admin --password admin -n < ./setup_kafka_ingestion.sql

    echo "3. Creating Iceberg Catalog in Doris..."
    docker exec doris-fe bash -c 'mysql -h 127.0.0.1 -P 9030 -u root -e "CREATE CATALOG IF NOT EXISTS iceberg_hadoop PROPERTIES (\"type\" = \"iceberg\", \"iceberg.catalog.type\" = \"hadoop\", \"warehouse\" = \"s3a://datalake/warehouse/\", \"s3.endpoint\" = \"http://minio:9000\", \"s3.access_key\" = \"admin\", \"s3.secret_key\" = \"admin12345\", \"s3.region\" = \"us-east-1\", \"use_path_style\" = \"true\", \"core.site.fs.s3a.endpoint\" = \"http://minio:9000\", \"core.site.fs.s3a.access.key\" = \"admin\", \"core.site.fs.s3a.secret.key\" = \"admin12345\", \"core.site.fs.s3a.path.style.access\" = \"true\", \"core.site.fs.s3a.impl\" = \"org.apache.hadoop.fs.s3a.S3AFileSystem\", \"core.site.fs.s3a.connection.ssl.enabled\" = \"false\", \"core.site.fs.s3a.signature.version\" = \"S3V4\");"'

    echo "4. Triggering Headless Airflow Ingestion..."
    echo " Waiting for Airflow to install requirements and initialize DB (<1min)..."
    while ! docker exec airflow airflow dags list &> /dev/null; do
        sleep 5
    done

    echo "⏳ Waiting for Scheduler to parse the Python DAG files..."
    while ! docker exec airflow airflow dags list 2>/dev/null | grep -q "taxi_ingestion_pipeline"; do
        sleep 3
    done

    echo "✅ Airflow is ready! Unpausing and triggering DAG..."
    # Unpause the DAG
    docker exec airflow airflow dags unpause taxi_ingestion_pipeline
    # Trigger the run
    docker exec airflow airflow dags trigger taxi_ingestion_pipeline
    # Check the status of the latest run
    echo "DAG Progress (Latest Run):"
    docker exec airflow airflow dags list-runs -d taxi_ingestion_pipeline | head -n 5

    echo "=== Pipeline Initialization Complete! ==="
    echo "Next: Run ./huynn.sh spark:jobs"
    ;;

  pipeline:verify)
    echo "=== Data Verification ==="
    echo ""
    echo "PostgreSQL (Source of Truth):"
    PG_TRIPS=$(docker exec postgres psql -U admin -d weather -t -c "SELECT COUNT(*) FROM yellow_taxi_trips;" 2>/dev/null | tr -d ' ')
    PG_WEATHER=$(docker exec postgres psql -U admin -d weather -t -c "SELECT COUNT(*) FROM nyc_weather_hourly;" 2>/dev/null | tr -d ' ')
    PG_ZONES=$(docker exec postgres psql -U admin -d weather -t -c "SELECT COUNT(*) FROM taxi_zones;" 2>/dev/null | tr -d ' ')
    echo "  Taxi trips: $PG_TRIPS"
    echo "  Weather records: $PG_WEATHER"
    echo "  Zone records: $PG_ZONES"
    echo ""
    echo "Iceberg (via Doris):"
    ICEBERG_SILVER=$(docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) as cnt FROM iceberg_hadoop.lakehouse.silver_taxi_trips;" 2>/dev/null | grep -E "^[0-9]" | tr -d '[:space:]' || echo "0")
    ICEBERG_GOLD=$(docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) as cnt FROM iceberg_hadoop.lakehouse.gold_hourly_metrics;" 2>/dev/null | grep -E "^[0-9]" | tr -d '[:space:]' || echo "0")
    ICEBERG_WEATHER=$(docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT COUNT(*) as cnt FROM iceberg_hadoop.lakehouse.nyc_weather;" 2>/dev/null | grep -E "^[0-9]" | tr -d '[:space:]' || echo "0")
    echo "  Silver taxi trips: $ICEBERG_SILVER"
    echo "  Gold hourly metrics: $ICEBERG_GOLD"
    echo "  Weather records: $ICEBERG_WEATHER"
    echo ""
    echo "ClickHouse (Real-time):"
    CH_COUNT=$(docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT COUNT(*) FROM lakehouse.taxi_prod" 2>/dev/null || echo "0")
    echo "  Taxi trips: $CH_COUNT"
    echo ""
    if [ "$PG_ZONES" = "265" ] && [ "$PG_TRIPS" -gt 0 ] 2>/dev/null; then
      echo "✓ Data pipeline is working"
    else
      echo "✗ Data pipeline needs initialization"
      echo "  Run: ./huynn.sh pipeline:init"
      echo "  Then: ./huynn.sh spark:jobs"
    fi
    ;;

  # ── Data ─────────────────────────────────────────────────
  data:verify)
    ./huynn.sh pipeline:verify ;;
  data:copy)
    docker cp /home/huynnz/GetAJob/Huynz/data/*.parquet airflow:/opt/airflow/data/ ;;
  data:ls)
    docker exec airflow ls -la /opt/airflow/data/ ;;

  # ── Simulators ──────────────────────────────────────────
  simulator:logs)
    echo "=== Taxi Simulator ==="
    docker logs taxi-simulator --tail 20
    echo ""
    echo "=== Weather Simulator ==="
    docker logs weather-simulator --tail 20
    ;;
  simulator:restart)
    cd /home/huynnz/GetAJob/Huynz && docker compose restart taxi-simulator weather-simulator
    echo "Simulators restarted. Check logs with:"
    echo "  ./huynn.sh simulator:logs"
    ;;

  # ── Help ───────────────────────────────────────────────────
  *)
    echo ""
    echo "Huynn Helper — NYC Taxi Analytics Pipeline"
    echo ""
    echo "Usage: ./huynn.sh <command> [args]"
    echo ""
    echo "Quick Start:"
    echo "  quickstart       - Full pipeline setup (start → init → verify)"
    echo ""
    echo "Stack Management:"
    echo "  up               - Start all services"
    echo "  down             - Stop all services"
    echo "  down-v           - Stop and remove all volumes (full reset)"
    echo "  restart <svc>    - Restart specific service"
    echo "  ps               - Show service status"
    echo "  logs <service>   - View logs (e.g., logs spark-master)"
    echo "  clean            - Clean up Docker resources (full reset)"
    echo ""
    echo "Pipeline Operations:"
    echo "  pipeline:init     - Initialize CDC pipeline (Debezium + Kafka)"
    echo "  pipeline:status   - Check overall pipeline health"
    echo "  pipeline:verify   - Verify data across all layers"
    echo ""
    echo "Debezium & CDC:"
    echo "  debezium:register - Register CDC connector"
    echo "  debezium:status   - Check connector status"
    echo "  debezium:restart  - Restart connector"
    echo "  debezium:delete   - Delete connector"
    echo ""
    echo "Kafka:"
    echo "  kafka:topics              - List all topics"
    echo "  kafka:consume <topic>     - Consume from beginning"
    echo "  kafka:consume-live <topic> - Live consumption"
    echo "  kafka:count <topic>       - Count messages"
    echo ""
    echo "Spark:"
    echo "  spark:jobs        - Start all CDC + Gold aggregation jobs"
    echo "  spark:apps        - List running applications"
    echo "  spark:logs        - View Spark master logs"
    echo "  spark:submit <py>  - Submit custom job"
    echo ""
    echo "Databases:"
    echo "  postgres:count    - Count taxi trips"
    echo "  postgres:weather  - Count weather records"
    echo "  postgres:range    - Show trip date range"
    echo "  postgres:zones    - Count taxi zones"
    echo "  clickhouse:count  - Count real-time records"
    echo "  clickhouse:tables - List ClickHouse tables"
    echo "  clickhouse:kafka-ingestion - Setup Kafka ingestion tables"
    echo ""
    echo "Doris / Iceberg:"
    echo "  doris:sh           - Open Doris SQL shell (mysql client)"
    echo "  doris:catalogs     - List Doris catalogs"
    echo "  doris:switch       - Switch to Iceberg catalog"
    echo "  doris:count <tbl>  - Count rows in table"
    echo "  doris:query <sql>  - Run SQL query"
    echo ""
    echo "API Testing:"
    echo "  api:health              - Health check"
    echo "  api:stats               - Real-time stats"
    echo "  api:stats-historical [start] [end] - Historical stats"
    echo "  api:zones [limit]       - Zone analytics"
    echo "  api:weather [hours]     - Weather impact (realtime)"
    echo "  api:weather-historical [start] [end] - Weather impact (historical)"
    echo "  api:test                - Test both modes"
    echo ""
    echo "Simulators:"
    echo "  simulator:logs     - View taxi and weather simulator logs"
    echo "  simulator:restart  - Restart simulators"
    echo ""
    echo "Weather:"
    echo "  weather:status    - Show weather data status (PG + Iceberg)"
    echo "  weather:count     - Count and show date range"
    echo "  weather:logs      - View weather simulator logs"
    echo ""
    echo "Dashboard:"
    echo "  dashboard:url     - Print dashboard URL"
    echo "  dashboard:logs   - View Streamlit logs"
    echo ""
    echo "Data:"
    echo "  data:verify - Verify data pipeline"
    echo "  data:copy   - Copy parquet files to Airflow"
    echo ""
    echo "Full Reset:"
    echo "  down-v           - Stop containers and remove all volumes"
    echo "  Then: ./huynn.sh quickstart"
    ;;
esac