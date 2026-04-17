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
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder.appName("GoldAggregations")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://datalake/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "admin12345")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.driver.extraClassPath",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    )
    .config(
        "spark.executor.extraClassPath",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/jars/postgresql-42.7.1.jar",
    )
    .getOrCreate()
)

sc = spark.sparkContext
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin12345")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")


def wait_for_silver(min_rows: int = 1000, poll_seconds: int = 30, max_attempts: int = 40):
    """
    Block until silver_taxi_trips has at least min_rows.
    Prevents the race condition where Gold starts before CDC has populated Silver.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            count = spark.sql(
                "SELECT COUNT(*) as n FROM lakehouse.lakehouse.silver_taxi_trips"
            ).collect()[0]["n"]
            logger.info(f"Silver row count: {count} (attempt {attempt}/{max_attempts})")
            if count >= min_rows:
                logger.info(f"Silver ready with {count} rows — proceeding.")
                return count
        except Exception as e:
            logger.warning(f"Could not query silver (attempt {attempt}): {e}")
        logger.info(f"Waiting {poll_seconds}s for Silver to populate...")
        time.sleep(poll_seconds)
    raise RuntimeError(
        f"Silver never reached {min_rows} rows after {max_attempts} attempts. "
        "Check TaxiMedallionCDC logs."
    )



def create_tables():
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.lakehouse.gold_hourly_metrics (
            metric_date      DATE,
            metric_hour      INT,
            pulocationid     INT,
            dolocationid     INT,
            borough          STRING,
            trip_count       LONG,
            total_revenue    DOUBLE,
            avg_fare         DOUBLE,
            avg_distance     DOUBLE,
            avg_passengers   DOUBLE,
            tip_sum          DOUBLE,
            tolls_sum        DOUBLE,
            peak_hour        BOOLEAN,
            processed_time   TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (metric_date)
        TBLPROPERTIES (
            'format-version'='2',
            'write.update.mode'='merge-on-read'
        )
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.lakehouse.gold_zone_performance (
            zone_id          INT,
            zone_name        STRING,
            borough          STRING,
            total_trips      LONG,
            total_revenue    DOUBLE,
            avg_fare         DOUBLE,
            avg_distance     DOUBLE,
            pickup_count     LONG,
            dropoff_count    LONG,
            last_updated     TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (borough)
        TBLPROPERTIES ('format-version'='2')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS lakehouse.lakehouse.gold_borough_summary (
            borough              STRING,
            metric_date          DATE,
            total_trips          LONG,
            total_revenue        DOUBLE,
            avg_fare             DOUBLE,
            avg_distance         DOUBLE,
            trip_count_pickup    LONG,
            trip_count_dropoff   LONG,
            last_updated         TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (metric_date)
        TBLPROPERTIES ('format-version'='2')
    """)
    logger.info("Gold tables verified/created.")

def load_zone_lookup():
    try:
        # Added the second .lakehouse
        return spark.sql("SELECT * FROM lakehouse.lakehouse.taxi_zones").alias("zones")
    except Exception as e:
        logger.warning(f"SQL zone load failed ({e}), trying DataFrame API...")
        # Added the second .lakehouse
        return spark.read.format("iceberg").load("lakehouse.lakehouse.taxi_zones").alias("zones")
    
def create_hourly_metrics(zone_lookup):
    silver_df = spark.sql("SELECT * FROM lakehouse.lakehouse.silver_taxi_trips")
    silver_with_pu_zone = silver_df.join(
        zone_lookup, col("pulocationid") == col("zones.LocationID"), "left"
    ).select(
        silver_df["*"],
        col("zones.Borough").alias("pu_borough"),
    )
    return (
        silver_with_pu_zone
        .groupBy(
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
            when(col("metric_hour").between(7, 9) | col("metric_hour").between(17, 19), True)
            .otherwise(False),
        )
        .withColumn("processed_time", current_timestamp())
    )


def create_zone_performance(zone_lookup):
    silver_df = spark.sql("SELECT * FROM lakehouse.lakehouse.silver_taxi_trips")
    pickup_metrics = (
        silver_df.groupBy("pulocationid")
        .agg(count("*").alias("pickup_count"), spark_sum("total_amount").alias("pickup_revenue"))
        .withColumnRenamed("pulocationid", "zone_id")
    )
    dropoff_metrics = (
        silver_df.groupBy("dolocationid")
        .agg(count("*").alias("dropoff_count"))
        .withColumnRenamed("dolocationid", "zone_id")
    )
    zone_metrics = (
        silver_df.groupBy("pulocationid")
        .agg(spark_avg("fare_amount").alias("avg_fare"), spark_avg("trip_distance").alias("avg_distance"))
        .withColumnRenamed("pulocationid", "zone_id")
    )
    return (
        pickup_metrics
        .join(dropoff_metrics, "zone_id", "outer")
        .join(zone_metrics, "zone_id", "left")
        .join(zone_lookup, col("zone_id") == col("LocationID"), "left")
        .select(
            col("zone_id"),
            col("Zone").alias("zone_name"),
            col("Borough").alias("borough"),
            col("pickup_count").alias("total_trips"),
            col("pickup_revenue").alias("total_revenue"),
            col("avg_fare"),
            col("avg_distance"),
            col("pickup_count"),
            col("dropoff_count"),
            current_timestamp().alias("last_updated"),
        )
    )


def create_borough_summary(zone_lookup):
    silver_df = spark.sql("SELECT * FROM lakehouse.lakehouse.silver_taxi_trips")
    silver_with_borough = silver_df.join(
        zone_lookup, silver_df["pulocationid"] == zone_lookup["LocationID"], "left"
    )
    return (
        silver_with_borough
        .groupBy(
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

def run_gold_aggregations(zone_lookup, label: str = "batch"):
    """
    Compute and write all three gold tables.
    Uses overwritePartitions() — safe to call repeatedly without duplicating rows.
    """
    hourly_df = create_hourly_metrics(zone_lookup)
    (
        hourly_df.writeTo("lakehouse.lakehouse.gold_hourly_metrics")
        .overwritePartitions()   # replaces affected date partitions only
    )
    logger.info(f"[{label}] gold_hourly_metrics written")

    zone_df = create_zone_performance(zone_lookup)
    (
        zone_df.writeTo("lakehouse.lakehouse.gold_zone_performance")
        .overwritePartitions()
    )
    logger.info(f"[{label}] gold_zone_performance written")

    borough_df = create_borough_summary(zone_lookup)
    (
        borough_df.writeTo("lakehouse.lakehouse.gold_borough_summary")
        .overwritePartitions()
    )
    logger.info(f"[{label}] gold_borough_summary written")


def process_batch(batch_df, batch_id):
    """
    Called by Structured Streaming every 300s.

    We do NOT check batch_df.isEmpty() here because our aggregations always
    do a full scan of silver_taxi_trips — the incoming batch_df only tells
    us there were *new* Silver rows since the last trigger, but the Gold
    tables need the full Silver history. Gating on isEmpty means we skip
    Gold entirely if Silver had no new rows during this window, even though
    Gold may never have been populated.
    """
    logger.info(f"Batch {batch_id}: triggering Gold aggregations...")
    try:
        zone_lookup = load_zone_lookup()
        run_gold_aggregations(zone_lookup, label=f"batch-{batch_id}")
    except Exception as e:
        logger.error(f"Batch {batch_id}: error — {e}")
        raise


if __name__ == "__main__":
    logger.info("=== Gold Layer Aggregations starting ===")

    # block until Silver is ready (avoid race condition)
    wait_for_silver(min_rows=1000, poll_seconds=30, max_attempts=40)

    # ensure tables exist
    create_tables()

    # one-shot backfill of all existing Silver data
    logger.info("Running initial backfill from Silver...")
    zone_lookup = load_zone_lookup()
    run_gold_aggregations(zone_lookup, label="backfill")
    logger.info("Backfill complete.")

    # streaming loop — refresh Gold whenever Silver gets new rows
    logger.info("Starting streaming trigger (every 300s)...")
    silver_stream = spark.readStream.format("iceberg").load(
        "lakehouse.lakehouse.silver_taxi_trips"
    )
    query = (
        silver_stream.writeStream
        .format("iceberg")
        .option("checkpointLocation", "s3a://datalake/checkpoints/gold_aggregations")
        .trigger(processingTime="300 seconds")
        .foreachBatch(process_batch)
        .start()
    )
    logger.info("Gold streaming started. Waiting for termination...")
    query.awaitTermination()