from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import (
    coalesce,
    get_json_object,
    when,
    lit,
    current_timestamp,
)
from pyspark.sql.types import *
from pyspark.sql import DataFrame
import logging
import requests
from pyspark import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLICKHOUSE_URL = "http://clickhouse:8123"
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "admin"
CLICKHOUSE_DB = "lakehouse"

sc = SparkContext.getOrCreate()
sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set(
    "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "admin")
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "admin12345")
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

spark = (
    SparkSession.builder.appName("TaxiCDCToIceberg")
    .config("spark.cores.max", "4")
    .config("spark.executor.cores", "2")
    .config("spark.default.parallelism", "8")
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
    .config(
        "spark.driver.extraClassPath",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    )
    .config(
        "spark.executor.extraClassPath",
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    )
    .getOrCreate()
)

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.taxi_prod (
        vendorid INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance FLOAT,
        ratecodeid INTEGER,
        store_and_fwd_flag STRING,
        pulocationid INTEGER,
        dolocationid INTEGER,
        payment_type INTEGER,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        airport_fee FLOAT,
        cbd_congestion_fee FLOAT
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2'
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.taxi_prod_deletes (
        vendorid INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        _deleted_at TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.silver_taxi_trips (
        vendorid INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INTEGER,
        trip_distance FLOAT,
        ratecodeid INTEGER,
        store_and_fwd_flag STRING,
        pulocationid INTEGER,
        dolocationid INTEGER,
        payment_type INTEGER,
        fare_amount FLOAT,
        extra FLOAT,
        mta_tax FLOAT,
        tip_amount FLOAT,
        tolls_amount FLOAT,
        improvement_surcharge FLOAT,
        total_amount FLOAT,
        congestion_surcharge FLOAT,
        airport_fee FLOAT,
        cbd_congestion_fee FLOAT
    )
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
""")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "postgres.public.yellow_taxi_trips")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", "false")
    .load()
)

parsed = df.selectExpr("CAST(value AS STRING) as raw_json")

raw_ops = parsed.withColumn("op", get_json_object(col("raw_json"), "$.op"))

inserts_updates = raw_ops.filter(col("op").isin("c", "u", "r"))

clean = inserts_updates.select(
    coalesce(
        get_json_object(col("raw_json"), "$.after.vendorid"),
        get_json_object(col("raw_json"), "$.after.VendorID"),
    )
    .cast("int")
    .alias("vendorid"),
    (
        get_json_object(col("raw_json"), "$.after.tpep_pickup_datetime").cast("double")
        / 1000000
    )
    .cast("timestamp")
    .alias("tpep_pickup_datetime"),
    (
        get_json_object(col("raw_json"), "$.after.tpep_dropoff_datetime").cast("double")
        / 1000000
    )
    .cast("timestamp")
    .alias("tpep_dropoff_datetime"),
    get_json_object(col("raw_json"), "$.after.passenger_count")
    .cast("int")
    .alias("passenger_count"),
    get_json_object(col("raw_json"), "$.after.trip_distance")
    .cast("float")
    .alias("trip_distance"),
    coalesce(
        get_json_object(col("raw_json"), "$.after.ratecodeid"),
        get_json_object(col("raw_json"), "$.after.RatecodeID"),
    )
    .cast("int")
    .alias("ratecodeid"),
    get_json_object(col("raw_json"), "$.after.store_and_fwd_flag").alias(
        "store_and_fwd_flag"
    ),
    coalesce(
        get_json_object(col("raw_json"), "$.after.pulocationid"),
        get_json_object(col("raw_json"), "$.after.PULocationID"),
    )
    .cast("int")
    .alias("pulocationid"),
    coalesce(
        get_json_object(col("raw_json"), "$.after.dolocationid"),
        get_json_object(col("raw_json"), "$.after.DOLocationID"),
    )
    .cast("int")
    .alias("dolocationid"),
    get_json_object(col("raw_json"), "$.after.payment_type")
    .cast("int")
    .alias("payment_type"),
    get_json_object(col("raw_json"), "$.after.fare_amount")
    .cast("float")
    .alias("fare_amount"),
    get_json_object(col("raw_json"), "$.after.extra").cast("float").alias("extra"),
    get_json_object(col("raw_json"), "$.after.mta_tax").cast("float").alias("mta_tax"),
    get_json_object(col("raw_json"), "$.after.tip_amount")
    .cast("float")
    .alias("tip_amount"),
    get_json_object(col("raw_json"), "$.after.tolls_amount")
    .cast("float")
    .alias("tolls_amount"),
    get_json_object(col("raw_json"), "$.after.improvement_surcharge")
    .cast("float")
    .alias("improvement_surcharge"),
    get_json_object(col("raw_json"), "$.after.total_amount")
    .cast("float")
    .alias("total_amount"),
    get_json_object(col("raw_json"), "$.after.congestion_surcharge")
    .cast("float")
    .alias("congestion_surcharge"),
    coalesce(
        get_json_object(col("raw_json"), "$.after.airport_fee"),
        get_json_object(col("raw_json"), "$.after.Airport_fee"),
    )
    .cast("float")
    .alias("airport_fee"),
    get_json_object(col("raw_json"), "$.after.cbd_congestion_fee")
    .cast("float")
    .alias("cbd_congestion_fee"),
    current_timestamp().alias("_processing_time"),
    col("op").alias("_operation"),
)

quality_checked = (
    clean.select(
        when(col("vendorid").isNull(), lit("REJECTED"))
        .otherwise("VALID")
        .alias("_quality_status"),
        when(col("tpep_pickup_datetime").isNull(), lit("REJECTED"))
        .otherwise("VALID")
        .alias("_pickup_status"),
        when(col("tpep_dropoff_datetime").isNull(), lit("REJECTED"))
        .otherwise("VALID")
        .alias("_dropoff_status"),
        when(col("fare_amount") < 0, lit("REJECTED"))
        .otherwise("VALID")
        .alias("_fare_status"),
        when(col("trip_distance") < 0, lit("REJECTED"))
        .otherwise("VALID")
        .alias("_distance_status"),
        "*",
    )
    .filter(col("_quality_status") == "VALID")
    .filter(col("_pickup_status") == "VALID")
    .filter(col("_dropoff_status") == "VALID")
    .filter(col("_fare_status") == "VALID")
    .filter(col("_distance_status") == "VALID")
    .drop(
        "_quality_status",
        "_pickup_status",
        "_dropoff_status",
        "_fare_status",
        "_distance_status",
    )
)

deletes = raw_ops.filter(col("op") == "d").select(
    coalesce(
        get_json_object(col("raw_json"), "$.before.vendorid"),
        get_json_object(col("raw_json"), "$.before.VendorID"),
    )
    .cast("int")
    .alias("vendorid"),
    (
        get_json_object(col("raw_json"), "$.before.tpep_pickup_datetime").cast("double")
        / 1000000
    )
    .cast("timestamp")
    .alias("tpep_pickup_datetime"),
    (
        get_json_object(col("raw_json"), "$.before.tpep_dropoff_datetime").cast(
            "double"
        )
        / 1000000
    )
    .cast("timestamp")
    .alias("tpep_dropoff_datetime"),
    current_timestamp().alias("_deleted_at"),
)


def write_to_clickhouse(df, table_name, batch_id):
    """
    Write DataFrame to ClickHouse using HTTP API
    """
    try:
        # Convert to Pandas for easier CSV generation
        pdf = df.toPandas()

        if pdf.empty:
            logger.info(
                f"Batch {batch_id}: No data to write to ClickHouse {table_name}"
            )
            return

        # Generate CSV format
        csv_data = pdf.to_csv(index=False, header=False)

        # Insert query
        query = f"INSERT INTO {CLICKHOUSE_DB}.{table_name} FORMAT CSV"

        # HTTP POST to ClickHouse
        response = requests.post(
            f"{CLICKHOUSE_URL}/?user={CLICKHOUSE_USER}&password={CLICKHOUSE_PASSWORD}&query={query}",
            data=csv_data.encode("utf-8"),
            timeout=30,
        )

        if response.status_code == 200:
            logger.info(
                f"Batch {batch_id}: Wrote {len(pdf)} records to ClickHouse {table_name}"
            )
        else:
            logger.error(f"Batch {batch_id}: ClickHouse write failed: {response.text}")
    except Exception as e:
        logger.error(
            f"Batch {batch_id}: Failed to write to ClickHouse {table_name}: {str(e)}"
        )


def write_to_iceberg(micro_batch_df, batch_id):
    if micro_batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} is empty, skipping")
        return

    inserts = micro_batch_df.filter(col("_operation").isin("c", "u", "r"))
    deletes_batch = micro_batch_df.filter(col("_operation") == "d")

    inserts_count = inserts.count()
    if inserts_count > 0:
        inserts_df = inserts.drop("_processing_time", "_operation")

        # Deduplicate by selecting distinct records based on key columns
        inserts_deduped = inserts_df.dropDuplicates(
            ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime"]
        )

        if inserts_deduped.count() > 0:
            # Cache for multiple writes
            inserts_deduped.cache()

            # Write to Iceberg (historical storage)
            inserts_deduped.writeTo("lakehouse.taxi_prod").append()
            logger.info(f"Batch {batch_id}: Wrote to Iceberg taxi_prod")

            inserts_deduped.writeTo("lakehouse.silver_taxi_trips").append()
            logger.info(f"Batch {batch_id}: Wrote to Iceberg silver_taxi_trips")

            # Write to ClickHouse (real-time queries)
            write_to_clickhouse(inserts_deduped, "silver_taxi_trips", batch_id)

            inserts_deduped.unpersist()
    else:
        logger.info(f"Batch {batch_id}: No inserts to process")

    deletes_count = deletes_batch.count()
    if deletes_count > 0:
        deletes_df = deletes_batch.select(
            "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "_deleted_at"
        )
        deletes_df.createOrReplaceTempView("deletes_batch")
        spark.sql("""
            MERGE INTO lakehouse.taxi_prod t
            USING deletes_batch s
            ON t.vendorid = s.vendorid 
            AND t.tpep_pickup_datetime = s.tpep_pickup_datetime
            AND t.tpep_dropoff_datetime = s.tpep_dropoff_datetime
            WHEN MATCHED THEN DELETE
        """)
        logger.info(f"Batch {batch_id}: Processed {deletes_count} deletes")


checkpoint_primary = "s3a://datalake/checkpoints/taxi_prod_cdc_stream"
checkpoint_backup = "s3a://datalake/checkpoints/taxi_prod_cdc_stream_backup"

query = (
    quality_checked.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_primary)
    .trigger(processingTime="30 seconds")
    .foreachBatch(write_to_iceberg)
    .start()
)

query.awaitTermination()
