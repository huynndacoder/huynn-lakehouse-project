from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaDebugger") \
    .config("spark.cores.max", "1") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "postgres.public.yellow_taxi_trips") \
    .option("startingOffsets", "earliest") \
    .load()

# Print the raw JSON exactly as it arrives from Kafka
query = df.selectExpr("CAST(value AS STRING) AS raw_json") \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()