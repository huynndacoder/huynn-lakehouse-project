# 🚕 NYC Taxi Data Pipeline — Full Stack

> A production-grade streaming data pipeline with real-time analytics dashboard.

---

## 📐 Architecture

```
                         ┌─────────────────────────────────────────┐
                         │           DATA SOURCES                  │
                         │  TLC Yellow Taxi (Parquet)              │
                         │  Open-Meteo Weather API                 │
                         └────────────┬────────────────────────────┘
                                      │ Airflow DAG (daily)
                                      ▼
┌──────────────┐   WAL    ┌──────────────────┐
│  PostgreSQL  │ ◄──────── │   Airflow DAG    │
│  (taxi_db)   │           │  (orchestration) │
└──────┬───────┘           └──────────────────┘
       │ Logical Replication (CDC)
       ▼
┌──────────────┐   Avro    ┌──────────────────┐
│   Debezium   │ ─────────►│ Schema Registry  │
│  (source)    │           │  (Avro schemas)  │
└──────┬───────┘           └──────────────────┘
       │ Kafka topic: taxi_db.public.taxi_trips
       ▼
┌──────────────┐
│    Kafka     │
│   (broker)   │
└──────┬───────┘
       │ Kafka Connect Sink
       ▼
┌──────────────┐  Iceberg  ┌──────────────────┐
│  Iceberg     │ ─────────►│  MinIO (S3)      │
│  Sink        │           │  s3://lakehouse/ │
└──────────────┘           └──────────────────┘
                                    │
                           ┌────────┴─────────┐
                           │  Nessie Catalog  │
                           │ (git versioning) │
                           └────────┬─────────┘
                                    │
                           ┌────────▼─────────┐
                           │   Spark Jobs     │
                           │ (aggregations)   │
                           └────────┬─────────┘
                                    │ JDBC write
                                    ▼
                           ┌──────────────────┐    ┌──────────────┐
                           │   PostgreSQL     │◄───│    Redis     │
                           │  (analytics)     │    │   (cache)    │
                           └────────┬─────────┘    └──────┬───────┘
                                    │                     │
                           ┌────────▼─────────────────────▼───────┐
                           │              FastAPI                  │
                           │        (REST analytics API)           │
                           └─────────────────┬─────────────────────┘
                                             │
                           ┌─────────────────▼─────────────────────┐
                           │            Streamlit                   │
                           │    (Real-time Analytics Dashboard)     │
                           └───────────────────────────────────────┘
```

---

## 🗂️ Project Structure

```
taxi-pipeline/
├── docker-compose.yml          # All 13 services
├── manage.sh                   # Helper CLI
├── connect/
│   └── Dockerfile              # Debezium + Iceberg connector image
├── spark/
│   ├── Dockerfile              # Spark + Iceberg + MinIO JARs
│   └── spark-defaults.conf     # Spark catalog/S3 config
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       ├── taxi_ingestion_pipeline.py   # Main ETL DAG
│       └── spark_analytics_dag.py       # Spark job DAG
├── fastapi/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py                 # REST API (10 endpoints)
├── streamlit/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                  # Multi-page dashboard
└── scripts/
    ├── init_postgres.sh         # Create multiple DBs
    ├── init_schema.sql          # Tables, indexes, publications
    ├── register_connectors.sh   # Debezium + Iceberg config
    └── spark_analytics.py       # PySpark aggregation job
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 24 with **at least 8GB RAM** allocated
- Docker Compose v2 (`docker compose` not `docker-compose`)
- 15GB free disk space

### Step 1 — Clone and start

```bash
git clone <your-repo>
cd taxi-pipeline
chmod +x manage.sh
./manage.sh start
```

First startup takes **5-10 minutes** — Docker is building 4 custom images and downloading connectors.

Watch progress:
```bash
docker compose logs -f connect connect-setup airflow-init
```

### Step 2 — Verify everything is up

```bash
./manage.sh status
./manage.sh urls
```

### Step 3 — Load data

Open **Airflow** at http://localhost:8084 (admin/admin):
1. Enable the `taxi_ingestion_pipeline` DAG
2. Click **Trigger DAG ▶**
3. Watch the tasks: `check_health → download_taxi_data → load_taxi_data → fetch_weather`

This loads ~50,000 NYC taxi trips into PostgreSQL.

### Step 4 — Open the dashboard

Visit **Streamlit** at http://localhost:8501 🎉

---

## 🔗 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Streamlit Dashboard** | http://localhost:8501 | — |
| **FastAPI Docs** | http://localhost:8000/docs | — |
| **Airflow** | http://localhost:8084 | admin / admin |
| **Redpanda Console** | http://localhost:8081 | — |
| **Kafka Connect API** | http://localhost:8083 | — |
| **Schema Registry** | http://localhost:8085 | — |
| **MinIO Console** | http://localhost:9001 | admin / password |
| **Nessie API** | http://localhost:19120/api/v1 | — |
| **Spark Master UI** | http://localhost:8082 | — |
| **PostgreSQL** | localhost:5432 | postgres / postgres |
| **Redis** | localhost:6379 | — |

---

## 📚 Component Deep-Dive (Educational)

### 1. PostgreSQL + WAL (Change Data Capture source)

PostgreSQL is configured with `wal_level=logical`, which means it writes a structured change log (Write-Ahead Log) that includes row-level INSERT/UPDATE/DELETE operations — not just block-level changes. Debezium reads this log.

Key config:
```sql
-- Created automatically by init_schema.sql:
CREATE PUBLICATION dbz_publication FOR TABLE taxi_trips;
```

### 2. Kafka (Message Streaming)

Running in **KRaft mode** (no ZooKeeper). Kafka acts as the durable, ordered buffer between Debezium (producer) and all consumers (Iceberg sink, your application).

Topic created: `taxi_db.public.taxi_trips`

Each message = one database change event (INSERT/UPDATE/DELETE).

### 3. Debezium (CDC Connector)

Debezium reads PostgreSQL's `pgoutput` replication plugin and translates WAL events into Kafka messages in near real-time. The `ExtractNewRecordState` transform unwraps Debezium's envelope so downstream consumers see clean row data.

### 4. Schema Registry + Avro

All messages are schema-validated using Apache Avro. The schema is automatically registered when Debezium first captures a row. This prevents schema drift from breaking downstream consumers.

### 5. Iceberg Sink Connector

The Tabular Iceberg Kafka Connect sink subscribes to the Kafka topic and writes data to Apache Iceberg table format in MinIO. Iceberg provides:
- ACID transactions on object storage
- Time travel (query historical snapshots)
- Schema evolution without rewriting files
- Efficient columnar storage (Parquet files)

### 6. MinIO (S3-compatible Storage)

MinIO stores the actual Iceberg Parquet data files. It speaks the AWS S3 API, so Spark and Iceberg use the same S3 client they'd use with real AWS.

Bucket: `s3://lakehouse/warehouse/`

### 7. Nessie (Iceberg Catalog)

Nessie tracks which Parquet files belong to which Iceberg table snapshot. It adds Git-like semantics: branches, commits, merges. You can create a `dev` branch, make changes, and merge to `main` — just like code.

### 8. Spark (Processing)

Spark reads Iceberg tables via the Nessie catalog and runs SQL aggregations. Results are written back to PostgreSQL via JDBC for the API to serve. Spark handles large-scale transformations that PostgreSQL can't do efficiently.

### 9. Airflow (Orchestration)

Airflow schedules and monitors the pipeline:
- `taxi_ingestion_pipeline` — daily download → load → weather
- `spark_analytics_dag` — hourly Spark aggregations

Each task is tracked, retried on failure, and audited.

### 10. FastAPI + Redis (API Layer)

FastAPI serves 10 analytics endpoints backed by PostgreSQL. Redis caches query results for 60 seconds — so the dashboard can refresh frequently without hammering the database.

### 11. Streamlit (Dashboard)

The dashboard has 7 pages:
- 📊 Overview — KPIs, revenue chart, distance distribution
- ⏱️ Temporal Analysis — hourly/daily patterns
- 🗺️ Zone Map — pydeck 3D map of pickup zones  
- 💳 Payment Analysis — pie/bar breakdown
- 🌤️ Weather Correlation — scatter plots
- 📡 Live Feed — real-time trip table
- 🔧 Pipeline Status — health check

---

## 🛠️ Troubleshooting

### Connect is unhealthy
```bash
docker compose logs connect | tail -50
# Check connector status:
curl http://localhost:8083/connectors?expand=status
```

### Airflow DB issues
```bash
docker compose logs airflow-init
# Re-init:
docker compose restart airflow-init airflow-webserver airflow-scheduler
```

### MinIO bucket missing
```bash
docker compose restart minio-setup
```

### Full reset
```bash
./manage.sh reset
./manage.sh start
```

---

## 📊 API Reference

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/summary` | Overall KPIs |
| `GET /api/v1/trips/hourly?days=7` | Hourly volume |
| `GET /api/v1/revenue/daily?days=30` | Daily revenue |
| `GET /api/v1/payments` | Payment breakdown |
| `GET /api/v1/zones/top` | Top pickup zones |
| `GET /api/v1/trips/distance-distribution` | Distance histogram |
| `GET /api/v1/trips/recent?limit=20` | Live feed |
| `GET /api/v1/weather/correlation` | Weather × trips |
| `GET /api/v1/vendors` | Vendor stats |
| `GET /api/v1/pipeline/status` | Health check |
