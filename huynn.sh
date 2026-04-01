#!/bin/bash
# huynn.sh — helper CLI
# Usage: ./huynn.sh <command>

CMD=$1
shift

case "$CMD" in

  # ── Airflow ──────────────────────────────────────────────
  airflow:sh)
    docker exec -it airflow bash "$@" ;;
  airflow:logs)
    docker logs -f airflow "$@" ;;
  airflow:restart)
    docker compose restart airflow ;;

  # ── Debezium ─────────────────────────────────────────────
  debezium:register)
    curl -s -X POST http://localhost:8083/connectors \
      -H "Content-Type: application/json" \
      -d @debezium.json | jq . ;;
  debezium:register-auto)
    # Auto-register with retry - waits for Debezium to be ready
    chmod +x ./register-debezium.sh
    ./register-debezium.sh ;;
  debezium:status)
    curl -s http://localhost:8083/connectors/postgres-taxi-connector/status | jq . ;;
  debezium:delete)
    curl -s -X DELETE http://localhost:8083/connectors/postgres-taxi-connector | jq . ;;
  debezium:list)
    curl -s http://localhost:8083/connectors | jq . ;;

  # ── Kafka ────────────────────────────────────────────────
  kafka:topics)
    docker exec -it kafka kafka-topics \
      --bootstrap-server kafka:9092 --list ;;
  kafka:consume)
    # Usage: ./huynn.sh kafka:consume postgres.public.yellow_taxi_trips
    docker exec -it kafka kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic "$1" \
      --from-beginning ;;
  kafka:consume-live)
    # Usage: ./huynn.sh kafka:consume-live postgres.public.yellow_taxi_trips
    # Live only (no from-beginning)
    docker exec -it kafka kafka-console-consumer \
      --bootstrap-server kafka:9092 \
      --topic "$1" ;;
  kafka:describe)
    docker exec -it kafka kafka-topics \
      --bootstrap-server kafka:9092 \
      --describe --topic "$1" ;;
  kafka:count)
    # Usage: ./huynn.sh kafka:count postgres.public.yellow_taxi_trips
    docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
      --broker-list kafka:9092 \
      --topic "$1" \
      --time -1 ;;
  kafka:watch-count)
    # Usage: ./huynn.sh kafka:watch-count postgres.public.yellow_taxi_trips
    watch "docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic $1 --time -1" ;;
  kafka:consumer-groups)
    docker exec kafka kafka-consumer-groups \
      --bootstrap-server kafka:9092 --list ;;
  kafka:consumer-status)
    # Usage: ./huynn.sh kafka:consumer-status <group-name>
    docker exec kafka kafka-consumer-groups \
      --bootstrap-server kafka:9092 \
      --describe --group "$1" ;;
  kafka:lag)
    # Check consumer lag for Spark CDC
    docker exec kafka kafka-consumer-groups \
      --bootstrap-server kafka:9092 \
      --describe --group connect-postgres-public-yellow_taxi_trips ;;

  # ── Spark ────────────────────────────────────────────────
  spark:sh)
    docker exec -it spark-master bash "$@" ;;
  spark:submit)
    docker exec -it spark-master \
      /opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --conf "spark.driver.extraClassPath=/opt/spark/jars/*" \
      --conf "spark.executor.extraClassPath=/opt/spark/jars/*" \
      /spark_jobs/"$1";;
  spark:logs)
    docker logs -f spark-master "$@" ;;

  # ── Postgres ─────────────────────────────────────────────
  postgres:sh)
    docker exec -it postgres psql -U admin -d weather "$@" ;;
  postgres:logs)
    docker logs -f postgres "$@" ;;

  # ── MinIO ────────────────────────────────────────────────
  minio:sh)
    docker exec -it minio bash "$@" ;;

  # ── Stack ────────────────────────────────────────────────
  up)
    docker compose up -d "$@" ;;
  down)
    docker compose down "$@" ;;
  restart)
    docker compose restart "$@" ;;
  ps)
    docker compose ps ;;
  logs)
    docker compose logs -f "$@" ;;
  sourcedata)
    docker exec -it airflow ls /opt/airflow/data ;;
  datacheck)
    docker exec -it airflow psql -U admin -d weather -c "SELECT COUNT(*) FROM yellow_taxi_trips;" ;;
  # ── Help ─────────────────────────────────────────────────
  *)
    echo ""
    echo "Huynn Helper — available commands:"
    echo ""
    echo "  Airflow:   airflow:sh | airflow:logs | airflow:restart"
    echo "  Debezium:  debezium:register | debezium:register-auto | debezium:status | debezium:delete | debezium:list"
    echo "  Kafka:"
    echo "    kafka:topics              - List all topics"
    echo "    kafka:consume <topic>    - Consume messages (from beginning)"
    echo "    kafka:consume-live <topic> - Consume only new messages"
    echo "    kafka:describe <topic>   - Describe topic details"
    echo "    kafka:count <topic>      - Count messages in topic"
    echo "    kafka:watch-count <topic> - Watch message count live"
    echo "    kafka:consumer-groups     - List consumer groups"
    echo "    kafka:consumer-status <group> - Describe consumer group"
    echo "    kafka:lag                - Check Spark CDC consumer lag"
    echo "  Spark:     spark:sh | spark:submit <job.py> | spark:logs"
    echo "  Postgres:  postgres:sh | postgres:logs"
    echo "  MinIO:     minio:sh"
    echo "  Stack:     up | down | restart | ps | logs [service]"
    echo ""
    ;;
esac