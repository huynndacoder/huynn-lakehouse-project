import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import current_timestamp, when, lit
from pyspark.sql.types import *
from pyspark import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    SparkSession.builder.appName("WeatherCDCToIceberg")
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

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.lakehouse.nyc_weather (
        time TIMESTAMP,
        temperature FLOAT,
        precipitation FLOAT,
        humidity FLOAT,
        windspeed FLOAT,
        _processing_time TIMESTAMP,
        _operation STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version'='2',
        'write.update.mode'='merge-on-read',
        'write.delete.mode'='merge-on-read',
        'write.merge.mode'='merge-on-read'
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.lakehouse.nyc_weather_deletes (
        time TIMESTAMP,
        _deleted_at TIMESTAMP
    )
    USING iceberg
    TBLPROPERTIES ('format-version'='2')
""")

BASE_SCHEMA = StructType(
    [
        StructField("op", StringType()),
        StructField(
            "after",
            StructType(
                [
                    StructField("time", LongType()),
                    StructField("temperature", FloatType()),
                    StructField("precipitation", FloatType()),
                    StructField("humidity", FloatType()),
                    StructField("windspeed", FloatType()),
                ]
            ),
            nullable=True,
        ),
        StructField(
            "before",
            StructType(
                [
                    StructField("time", LongType()),
                ]
            ),
            nullable=True,
        ),
    ]
)


def get_dynamic_schema(kafka_df):
    try:
        raw_value = kafka_df.selectExpr("CAST(value AS STRING) as raw_json").first()
        if raw_value:
            sample_json = raw_value["raw_json"]
            if sample_json:
                parsed = json.loads(sample_json)
                after_data = parsed.get("after", {})
                if after_data:
                    dynamic_fields = []
                    for key, value in after_data.items():
                        if key == "time":
                            dtype = LongType()
                        elif isinstance(value, float):
                            dtype = FloatType()
                        elif isinstance(value, int):
                            dtype = IntegerType()
                        else:
                            dtype = StringType()
                        dynamic_fields.append(StructField(key, dtype, True))

                    if dynamic_fields:
                        return StructType(
                            [
                                StructField("op", StringType()),
                                StructField("after", StructType(dynamic_fields), True),
                                StructField(
                                    "before",
                                    StructType([StructField("time", LongType(), True)]),
                                    True,
                                ),
                            ]
                        )
    except Exception as e:
        logger.warning(f"Could not infer dynamic schema: {e}")

    return BASE_SCHEMA


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "postgres.public.nyc_weather_hourly")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

schema = BASE_SCHEMA

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select(
    "data.*"
)

raw_ops = parsed.withColumn("op", col("op"))

inserts_updates = raw_ops.filter(col("op").isin("c", "u", "r"))

clean = inserts_updates.filter(col("after").isNotNull()).select(
    (col("after.time") / 1000000).cast("timestamp").alias("time"),
    col("after.temperature"),
    col("after.precipitation"),
    col("after.humidity"),
    col("after.windspeed"),
    current_timestamp().alias("_processing_time"),
    col("op").alias("_operation"),
)

quality_checked = (
    clean.select(
        when(col("time").isNull(), lit("REJECTED"))
        .otherwise("VALID")
        .alias("_time_status"),
        when(col("temperature").isNull(), lit("REJECTED"))
        .otherwise("VALID")
        .alias("_temp_status"),
        when((col("temperature") < -100) | (col("temperature") > 100), lit("REJECTED"))
        .otherwise("VALID")
        .alias("_temp_range"),
        "*",
    )
    .filter(col("_time_status") == "VALID")
    .filter(col("_temp_status") == "VALID")
    .filter(col("_temp_range") == "VALID")
    .drop("_time_status", "_temp_status", "_temp_range")
)

deletes = raw_ops.filter(col("op") == "d").select(
    (col("before.time") / 1000000).cast("timestamp").alias("time"),
    current_timestamp().alias("_deleted_at"),
)


def write_weather_to_iceberg(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        logger.info(f"Batch {batch_id} is empty, skipping")
        return

    inserts = micro_batch_df.filter(col("_operation").isin("c", "u", "r"))
    deletes_batch = micro_batch_df.filter(col("_operation") == "d")

    if not inserts.isEmpty():
        inserts_df = inserts.select(
            "time",
            "temperature",
            "precipitation",
            "humidity",
            "windspeed",
            "_processing_time",
            "_operation"
        )
        
        # append() instead of overwritePartitions() for unpartitioned tables
        inserts_df.writeTo("lakehouse.lakehouse.nyc_weather").append()
        logger.info(
            f"Batch {batch_id}: Wrote {inserts_df.count()} inserts/updates to nyc_weather"
        )

    if not deletes_batch.isEmpty():
        delete_count = deletes_batch.count()
        deletes_df = deletes_batch.select("time", "_deleted_at")
        deletes_df.createOrReplaceTempView("weather_deletes_batch")
        spark.sql("""
            MERGE INTO lakehouse.lakehouse.nyc_weather t
            USING weather_deletes_batch s
            ON t.time = s.time
            WHEN MATCHED THEN DELETE
        """)
        logger.info(f"Batch {batch_id}: Processed {delete_count} weather deletes")

checkpoint_primary = "s3a://datalake/checkpoints/weather_v4"
checkpoint_backup = "s3a://datalake/checkpoints/weather_v4_backup"

query = (
    quality_checked.writeStream.outputMode("append")
    .option("checkpointLocation", checkpoint_primary)
    .trigger(processingTime="30 seconds")
    .foreachBatch(write_weather_to_iceberg)
    .start()
)

query.awaitTermination()
