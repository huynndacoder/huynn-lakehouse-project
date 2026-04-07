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
    sleep 30
    echo ""
    echo "[2/5] Checking services..."
    docker compose ps
    echo ""
    echo "[3/5] Initializing CDC pipeline..."
    ./scripts/fix_pipeline.sh
    echo ""
    echo "[4/5] Starting Spark jobs..."
    ./start_spark_jobs.sh
    echo ""
    echo "[5/5] Verifying data..."
    echo "PostgreSQL:"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips FROM yellow_taxi_trips;" 2>/dev/null || echo "  Not accessible"
    echo ""
    echo "ClickHouse:"
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT COUNT(*) as count FROM lakehouse.taxi_prod" 2>/dev/null || echo "  Not accessible"
    echo ""
    echo "=== Quick Start Complete ==="
    echo ""
    echo "Access dashboard: http://localhost:8505"
    echo "Check pipeline: ./huynn.sh pipeline:status"
    ;;

  # ── Stack Management ────────────────────────────────────────
  up)
    cd /home/huynnz/GetAJob/Huynz && docker compose up -d "$@" ;;
  down)
    cd /home/huynnz/GetAJob/Huynz && docker compose down "$@" ;;
  restart)
    cd /home/huynnz/GetAJob/Huynz && docker compose restart "$@" ;;
  ps)
    cd /home/huynnz/GetAJob/Huynz && docker compose ps ;;
  logs)
    cd /home/huynnz/GetAJob/Huynz && docker compose logs -f "$@" ;;
  status)
    cd /home/huynnz/GetAJob/Huynz && docker compose ps ;;
  clean)
    echo "Cleaning up Docker resources..."
    docker system prune -af --volumes
    docker builder prune -af
    echo "Done. Run './huynn.sh up' to restart." ;;

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
    docker exec airflow airflow dags unpause weather_to_postgres 2>/dev/null || true
    docker exec airflow airflow dags unpause spark_cdc_medallion_pipeline 2>/dev/null || true
    echo "Done. DAGs unpasued." ;;

  # ── Debezium ─────────────────────────────────────────────
  debezium:register)
    curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @/home/huynnz/GetAJob/Huynz/debezium.json | jq . ;;
  debezium:status)
    curl -s http://localhost:8083/connectors/postgres-taxi-connector/status | jq . ;;
  debezium:delete)
    curl -s -X DELETE http://localhost:8083/connectors/postgres-taxi-connector | jq . ;;
  debezium:list)
    curl -s http://localhost:8083/connectors | jq . ;;
  debezium:restart)
    curl -s -X POST http://localhost:8083/connectors/postgres-taxi-connector/restart | jq . ;;

  # ── Kafka ────────────────────────────────────────────────
  kafka:topics)
    docker exec -it kafka kafka-topics \
      --bootstrap-server kafka:9092 --list ;;
  kafka:consume)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:consume <topic>"
      exit 1
    fi
    docker exec -it kafka kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic "$1" \
      --from-beginning ;;
  kafka:consume-live)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:consume-live <topic>"
      exit 1
    fi
    docker exec -it kafka kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic "$1" ;;
  kafka:describe)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:describe <topic>"
      exit 1
    fi
    docker exec -it kafka kafka-topics \
      --bootstrap-server kafka:9092 \
      --describe --topic "$1" ;;
  kafka:count)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:count <topic>"
      exit 1
    fi
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list kafka:9092 \
      --topic "$1" \
      --time -1 ;;
  kafka:lag)
    docker exec kafka kafka-consumer-groups \
      --bootstrap-server kafka:9092 \
      --describe --group connect-postgres-public-yellow_taxi_trips ;;
  kafka:consumer-groups)
    docker exec kafka kafka-consumer-groups \
      --bootstrap-server kafka:9092 --list ;;
  kafka:consumer-status)
    if [ -z "$1" ]; then
      echo "Usage: ./huynn.sh kafka:consumer-status <group-name>"
      exit 1
    fi
    docker exec kafka kafka-consumer-groups \
      --bootstrap-server kafka:9092 \
      --describe --group "$1" ;;

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
    docker exec -it spark-master \
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
      --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
      /spark_jobs/"$1" ;;
  spark:logs)
    docker logs -f spark-master "$@" ;;
  spark:worker:logs)
    docker logs -f spark-worker "$@" ;;
  spark:apps)
    curl -s http://localhost:8090/api/v1/applications | jq '.[].name' ;;
  spark:jobs)
    ./start_spark_jobs.sh ;;

  # ── PostgreSQL ─────────────────────────────────────────────
  postgres:sh)
    docker exec -it postgres psql -U admin -d weather "$@" ;;
  postgres:logs)
    docker logs -f postgres "$@" ;;
  postgres:count)
    docker exec -it postgres psql -U admin -d weather -c "SELECT COUNT(*) as total_trips FROM yellow_taxi_trips;" ;;
  postgres:weather)
    docker exec -it postgres psql -U admin -d weather -c "SELECT COUNT(*) as total_weather FROM nyc_weather_hourly;" ;;
  postgres:range)
    docker exec -it postgres psql -U admin -d weather -c "SELECT COUNT(*) as total, MIN(tpep_pickup_datetime) as min_date, MAX(tpep_pickup_datetime) as max_date FROM yellow_taxi_trips;" ;;
  postgres:recent)
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips_last_hour FROM yellow_taxi_trips WHERE tpep_pickup_datetime >= NOW() - INTERVAL '1 hour';" ;;

  # ── ClickHouse ─────────────────────────────────────────────
  clickhouse:sh)
    docker exec -it clickhouse clickhouse-client --user admin --password admin ;;
  clickhouse:count)
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT count() as realtime_count FROM lakehouse.taxi_prod" ;;
  clickhouse:tables)
    docker exec clickhouse clickhouse-client -q "SHOW TABLES FROM lakehouse" ;;
  clickhouse:range)
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT count() as count, MIN(tpep_pickup_datetime) as min_time, MAX(tpep_pickup_datetime) as max_time FROM lakehouse.taxi_prod" ;;

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
    curl -s http://localhost:8001/health | jq . ;;
  api:stats)
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" \
      "http://localhost:8001/api/v1/dashboard/stats?mode=realtime" | jq . ;;
  api:stats-historical)
    START_DATE="${1:-2025-01-01}"
    END_DATE="${2:-2025-01-31}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" \
      "http://localhost:8001/api/v1/dashboard/stats?mode=historical&start_date=${START_DATE}&end_date=${END_DATE}" | jq . ;;
  api:zones)
    LIMIT="${1:-10}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" \
      "http://localhost:8001/api/v1/analytics/zones?mode=realtime&limit=${LIMIT}" | jq . ;;
  api:weather)
    HOURS="${1:-24}"
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" \
      "http://localhost:8001/api/v1/analytics/weather-impact?mode=realtime&hours_back=${HOURS}" | jq . ;;
  api:test)
    echo "Testing historical mode (Jan 2025)..."
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" \
      "http://localhost:8001/api/v1/dashboard/stats?mode=historical&start_date=2025-01-01&end_date=2025-01-07" | jq .
    echo ""
    echo "Testing real-time mode..."
    curl -s -H "X-API-Key: huynz-super-secret-key-2026" \
      "http://localhost:8001/api/v1/dashboard/stats?mode=realtime" | jq . ;;

  # ── Dashboard ───────────────────────────────────────────
  dashboard:url)
    echo "Dashboard URL: http://localhost:8505" ;;
  dashboard:logs)
    docker logs -f analytics-dashboard "$@" ;;
  dashboard:restart)
    cd /home/huynnz/GetAJob/Huynz && docker compose restart analytics-dashboard ;;

  # ── Weather ─────────────────────────────────────────────
  weather:backfill)
    echo "Running historical weather backfill..."
    ./scripts/backfill_weather.sh ;;
  weather:count)
    docker exec -it postgres psql -U admin -d weather -c "SELECT COUNT(*) as count, MIN(time) as start_date, MAX(time) as end_date FROM nyc_weather_hourly;" ;;
  weather:range)
    docker exec -it postgres psql -U admin -d weather -c "SELECT MIN(time) as min_time, MAX(time) as max_time, COUNT(*) as records FROM nyc_weather_hourly;" ;;
  weather:logs)
    docker logs -f weather-simulator "$@" ;;

  # ── Pipeline ─────────────────────────────────────────────
  pipeline:status)
    echo "=== Pipeline Health Check ==="
    echo ""
    echo "PostgreSQL (Source):"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as trips FROM yellow_taxi_trips;" 2>/dev/null || echo "  [ERROR] PostgreSQL not accessible"
    docker exec postgres psql -U admin -d weather -c "SELECT COUNT(*) as weather FROM nyc_weather_hourly;" 2>/dev/null
    docker exec postgres psql -U admin -d weather -c "SELECT MIN(tpep_pickup_datetime) as min, MAX(tpep_pickup_datetime) as max FROM yellow_taxi_trips;" 2>/dev/null || true
    echo ""
    echo "Kafka Topics:"
    docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list 2>/dev/null | grep -E "postgres\." || echo "  [WARN] No CDC topics found"
    echo ""
    echo "Debezium:"
    curl -s http://localhost:8083/connectors/postgres-taxi-connector/status 2>/dev/null | jq -r '.connector.state' || echo "  [ERROR] Connector not registered"
    echo ""
    echo "ClickHouse:"
    docker exec clickhouse clickhouse-client --user admin --password admin -q "SELECT COUNT(*) as count FROM lakehouse.taxi_prod" 2>/dev/null || echo "  [ERROR] ClickHouse not accessible"
    echo ""
    echo "Spark Applications:"
    curl -s http://localhost:8090/api/v1/applications 2>/dev/null | grep -o '"name":"[^"]*"' | head -3 || echo "  [WARN] No apps running"
    echo ""
    echo "API Health:"
    curl -s http://localhost:8001/health 2>/dev/null | jq -r '.message' || echo "  [ERROR] API not accessible"
    echo ""
    echo "Weather Simulator:"
    docker logs weather-simulator --tail 5 2>/dev/null | grep -E "Weather updated|Connected" || echo "  [WARN] Not running"
    ;;

  pipeline:init)
    ./scripts/fix_pipeline.sh
    ;;

  pipeline:verify)
    echo "=== Data Verification ==="
    echo ""
    echo "PostgreSQL (Source of Truth):"
    POSTGRES_COUNT=$(docker exec postgres psql -U admin -d weather -t -c "SELECT COUNT(*) FROM yellow_taxi_trips;" 2>/dev/null | tr -d ' ')
    echo "  Taxi trips: $POSTGRES_COUNT"
    WEATHER_COUNT=$(docker exec postgres psql -U admin -d weather -t -c "SELECT COUNT(*) FROM nyc_weather_hourly;" 2>/dev/null | tr -d ' ')
    echo "  Weather records: $WEATHER_COUNT"
    echo ""
    echo "ClickHouse (Real-time):"
    CH_COUNT=$(docker exec clickhouse clickhouse-client -q "SELECT COUNT(*) FROM lakehouse.taxi_prod" 2>/dev/null)
    echo "  Taxi trips: $CH_COUNT"
    echo ""
    if [ "$POSTGRES_COUNT" -gt 0 ] && [ "$CH_COUNT" -gt 0 ]; then
      echo "✓ Data pipeline is working"
    else
      echo "✗ Data pipeline needs initialization"
      echo "  Run: ./huynn.sh pipeline:init"
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
    echo "  restart <svc>    - Restart specific service"
    echo "  ps               - Show service status"
    echo "  logs <service>   - View logs (e.g., logs spark-master)"
    echo "  clean            - Clean up Docker resources"
    echo ""
    echo "Pipeline Operations:"
    echo "  pipeline:init    - Initialize CDC pipeline (Debezium + Kafka)"
    echo "  pipeline:status   - Check overall pipeline health"
    echo "  pipeline:verify   - Compare PostgreSQL vs ClickHouse counts"
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
    echo "  spark:jobs        - Start all CDC jobs"
    echo "  spark:apps        - List running applications"
    echo "  spark:logs        - View Spark master logs"
    echo "  spark:submit <py>  - Submit custom job"
    echo ""
    echo "Databases:"
    echo "  postgres:count    - Count taxi trips"
    echo "  postgres:weather  - Count weather records"
    echo "  postgres:range    - Show trip date range"
    echo "  clickhouse:count  - Count real-time records"
    echo "  clickhouse:tables - List tables"
    echo ""
    echo "API Testing:"
    echo "  api:health              - Health check"
    echo "  api:stats               - Real-time stats"
    echo "  api:stats-historical [start] [end] - Historical stats"
    echo "  api:zones [limit]       - Zone analytics"
    echo "  api:weather [hours]     - Weather impact"
    echo "  api:test                - Test both modes"
    echo ""
    echo "Simulators:"
    echo "  simulator:logs     - View taxi and weather simulator logs"
    echo "  simulator:restart  - Restart simulators"
    echo ""
    echo "Weather:"
    echo "  weather:backfill  - Backfill historical weather"
    echo "  weather:count     - Count and show date range"
    echo "  weather:logs      - View weather simulator logs"
    echo ""
    echo "Dashboard:"
    echo "  dashboard:url     - Print dashboard URL"
    echo "  dashboard:logs    - View Streamlit logs"
    echo ""
    echo "Data:"
    echo "  data:verify - Verify data pipeline"
    echo "  data:copy   - Copy parquet files to Airflow"
    echo ""
    ;;
esac