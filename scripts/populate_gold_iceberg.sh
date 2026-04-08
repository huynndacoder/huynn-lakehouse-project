#!/bin/bash
set -e

echo "=== Populating Iceberg Gold Tables from Silver ==="

echo "Step 1: Loading taxi_zones to Iceberg..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://datalake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /spark_jobs/load_zones.py

echo "Step 2: Populating Gold tables from Silver..."
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lakehouse=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lakehouse.type=hadoop \
  --conf spark.sql.catalog.lakehouse.warehouse=s3a://datalake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /spark_jobs/gold_aggregations.py

echo "=== Gold Tables Populated Successfully ==="
echo "You can now query Iceberg Gold tables via Trino:"
echo "  docker exec -it trino trino-cli --catalog iceberg --schema lakehouse"
echo "  SELECT * FROM gold_zone_performance LIMIT 5;"