from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("WeatherCDCToIceberg")
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
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.nyc_weather (
        time TIMESTAMP,
        temperature FLOAT,
        precipitation FLOAT,
        humidity FLOAT,
        windspeed FLOAT
    )
    USING iceberg
    TBLPROPERTIES ('format-version'='2', 'write.update.mode'='merge-on-read')
""")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "postgres.public.nyc_weather_hourly")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

schema = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField(
                        "after",
                        StructType(
                            [
                                StructField(
                                    "time", LongType()
                                ),  # Debezium Microseconds
                                StructField("temperature", FloatType()),
                                StructField("precipitation", FloatType()),
                                StructField("humidity", FloatType()),
                                StructField("windspeed", FloatType()),
                            ]
                        ),
                    ),
                    StructField("op", StringType()),
                ]
            ),
        )
    ]
)

parsed = df.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("data")
)

clean = (
    parsed.select("data.payload.*")
    .filter(col("op").isin("c", "u", "r"))
    .filter(col("after").isNotNull())
    .select(
        (col("after.time") / 1000000).cast("timestamp").alias("time"),
        col("after.temperature"),
        col("after.precipitation"),
        col("after.humidity"),
        col("after.windspeed"),
    )
)

query = (
    clean.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", "s3a://datalake/checkpoints/weather_v4")
    .trigger(processingTime="60 seconds")
    .toTable("lakehouse.nyc_weather")
)

query.awaitTermination()
