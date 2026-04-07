"""
Gold Layer Aggregations Spark Job

Reads from silver_taxi_trips and creates pre-computed aggregations
for the serving layer (dashboards, APIs).

Tables created:
- gold.taxi_hourly_metrics: Hourly aggregations by zone
- gold.zone_performance: Zone-level performance metrics
- gold.borough_summary: Borough-level aggregations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    to_date,
    count,
    sum as spark_sum,
    avg as spark_avg,
    max as spark_max,
    min as spark_min,
    current_timestamp,
    when,
    lit,
)
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("GoldAggregations").getOrCreate()

sc = spark.sparkContext
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin12345")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_hourly_metrics (
        metric_date DATE,
        metric_hour INT,
        pulocationid INT,
        dolocationid INT,
        borough STRING,
        trip_count LONG,
        total_revenue DOUBLE,
        avg_fare DOUBLE,
        avg_distance DOUBLE,
        avg_passengers DOUBLE,
        tip_sum DOUBLE,
        tolls_sum DOUBLE,
        peak_hour BOOLEAN,
        processed_time TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read'
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_zone_performance (
        zone_id INT,
        zone_name STRING,
        borough STRING,
        total_trips LONG,
        total_revenue DOUBLE,
        avg_fare DOUBLE,
        avg_distance DOUBLE,
        pickup_count LONG,
        dropoff_count LONG,
        last_updated TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2'
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.gold_borough_summary (
        borough STRING,
        metric_date DATE,
        total_trips LONG,
        total_revenue DOUBLE,
        avg_fare DOUBLE,
        avg_distance DOUBLE,
        trip_count_pickup LONG,
        trip_count_dropoff LONG,
        last_updated TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2'
    )
""")

zone_lookup = spark.read.format("iceberg").load("lakehouse.taxi_zones").alias("zones")


def create_hourly_metrics():
    """Create hourly aggregations from silver_taxi_trips."""
    logger.info("Processing hourly metrics...")

    silver_df = spark.read.format("iceberg").load("lakehouse.silver_taxi_trips")

    silver_with_pu_zone = silver_df.join(
        zone_lookup, col("pulocationid") == col("zones.LocationID"), "left"
    ).select(
        silver_df["*"],
        col("zones.Borough").alias("pu_borough"),
        col("zones.Zone").alias("pu_zone"),
    )

    hourly_metrics = (
        silver_with_pu_zone.groupBy(
            to_date(col("tpep_pickup_datetime")).alias("metric_date"),
            hour(col("tpep_pickup_datetime")).alias("metric_hour"),
            col("pulocationid"),
            col("dolocationid"),
            col("pu_borough").alias("borough"),
        )
        .agg(
            count("*").alias("trip_count"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_avg("fare_amount").alias("avg_fare"),
            spark_avg("trip_distance").alias("avg_distance"),
            spark_avg("passenger_count").alias("avg_passengers"),
            spark_sum("tip_amount").alias("tip_sum"),
            spark_sum("tolls_amount").alias("tolls_sum"),
        )
        .withColumn(
            "peak_hour",
            when(
                col("metric_hour").between(7, 9) | col("metric_hour").between(17, 19),
                True,
            ).otherwise(False),
        )
        .withColumn("processed_time", current_timestamp())
    )

    return hourly_metrics


def create_zone_performance():
    """Create zone-level performance metrics."""
    logger.info("Processing zone performance...")

    silver_df = spark.read.format("iceberg").load("lakehouse.silver_taxi_trips")

    pickup_metrics = (
        silver_df.groupBy("pulocationid")
        .agg(
            count("*").alias("pickup_count"),
            spark_sum("total_amount").alias("pickup_revenue"),
        )
        .withColumnRenamed("pulocationid", "zone_id")
    )

    dropoff_metrics = (
        silver_df.groupBy("dolocationid")
        .agg(count("*").alias("dropoff_count"))
        .withColumnRenamed("dolocationid", "zone_id")
    )

    zone_metrics = (
        silver_df.groupBy("pulocationid")
        .agg(
            spark_avg("fare_amount").alias("avg_fare"),
            spark_avg("trip_distance").alias("avg_distance"),
        )
        .withColumnRenamed("pulocationid", "zone_id")
    )

    zone_perf = (
        pickup_metrics.join(dropoff_metrics, "zone_id", "outer")
        .join(zone_metrics, "zone_id", "left")
        .join(zone_lookup, col("zone_id") == col("LocationID"), "left")
        .select(
            col("zone_id"),
            col("Zone").alias("zone_name"),
            col("Borough").alias("borough"),
            spark_sum("pickup_count").alias("total_trips"),
            col("pickup_revenue").alias("total_revenue"),
            col("avg_fare"),
            col("avg_distance"),
            col("pickup_count"),
            col("dropoff_count"),
            current_timestamp().alias("last_updated"),
        )
    )

    return zone_perf


def create_borough_summary():
    """Create borough-level aggregations."""
    logger.info("Processing borough summary...")

    silver_df = spark.read.format("iceberg").load("lakehouse.silver_taxi_trips")

    silver_with_borough = silver_df.join(
        zone_lookup, silver_df["pulocationid"] == zone_lookup["LocationID"], "left"
    )

    borough_summary = (
        silver_with_borough.groupBy(
            col("Borough").alias("borough"),
            to_date(col("tpep_pickup_datetime")).alias("metric_date"),
        )
        .agg(
            count("*").alias("total_trips"),
            spark_sum("total_amount").alias("total_revenue"),
            spark_avg("fare_amount").alias("avg_fare"),
            spark_avg("trip_distance").alias("avg_distance"),
        )
        .withColumn("trip_count_pickup", col("total_trips"))
        .withColumn("trip_count_dropoff", col("total_trips"))
        .withColumn("last_updated", current_timestamp())
    )

    return borough_summary


def process_batch(batch_df, batch_id):
    """Process each micro-batch for streaming aggregations."""
    if batch_df.isEmpty():
        logger.info(f"Batch {batch_id}: No new data, skipping")
        return

    logger.info(f"Batch {batch_id}: Processing gold layer aggregations...")

    try:
        hourly_df = create_hourly_metrics()
        hourly_df.writeTo("lakehouse.gold_hourly_metrics").append()
        logger.info(f"Batch {batch_id}: Updated gold_hourly_metrics")

        zone_df = create_zone_performance()
        zone_df.writeTo("lakehouse.gold_zone_performance").append()
        logger.info(f"Batch {batch_id}: Updated gold_zone_performance")

        borough_df = create_borough_summary()
        borough_df.writeTo("lakehouse.gold_borough_summary").append()
        logger.info(f"Batch {batch_id}: Updated gold_borough_summary")

    except Exception as e:
        logger.error(f"Batch {batch_id}: Error processing gold layer: {e}")
        raise


if __name__ == "__main__":
    logger.info("Starting Gold Layer Aggregations...")
    logger.info("Running batch processing on existing silver data...")

    try:
        hourly_df = create_hourly_metrics()
        hourly_df.writeTo("lakehouse.gold_hourly_metrics").append()
        logger.info("Initial gold_hourly_metrics created")

        zone_df = create_zone_performance()
        zone_df.writeTo("lakehouse.gold_zone_performance").append()
        logger.info("Initial gold_zone_performance created")

        borough_df = create_borough_summary()
        borough_df.writeTo("lakehouse.gold_borough_summary").append()
        logger.info("Initial gold_borough_summary created")

        logger.info("Gold layer initialization complete!")

        silver_stream = spark.readStream.format("iceberg").load(
            "lakehouse.silver_taxi_trips"
        )

        checkpoint_location = "s3a://datalake/checkpoints/gold_aggregations"

        query = (
            silver_stream.writeStream.format("iceberg")
            .option("checkpointLocation", checkpoint_location)
            .trigger(processingTime="300 seconds")
            .foreachBatch(process_batch)
            .start()
        )

        logger.info("Gold aggregation streaming started. Waiting for data...")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Fatal error in gold aggregations: {e}")
        raise
