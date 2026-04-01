from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import coalesce, get_json_object
from pyspark.sql.types import *

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
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


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

# REMOVED .payload from every single get_json_object call!
clean = (
    parsed.withColumn("op", get_json_object(col("raw_json"), "$.op"))
    .filter(col("op").isin("c", "u", "r"))
    .select(
        coalesce(
            get_json_object(col("raw_json"), "$.after.vendorid"),
            get_json_object(col("raw_json"), "$.after.VendorID"),
        )
        .cast("int")
        .alias("vendorid"),
        (
            get_json_object(col("raw_json"), "$.after.tpep_pickup_datetime").cast(
                "double"
            )
            / 1000000
        )
        .cast("timestamp")
        .alias("tpep_pickup_datetime"),
        (
            get_json_object(col("raw_json"), "$.after.tpep_dropoff_datetime").cast(
                "double"
            )
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
        get_json_object(col("raw_json"), "$.after.mta_tax")
        .cast("float")
        .alias("mta_tax"),
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
    )
)

query = (
    clean.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://datalake/checkpoints/taxi_prod_cdc_stream")
    .trigger(processingTime="30 seconds")
    .toTable("lakehouse.taxi_prod")
)
query.awaitTermination()
