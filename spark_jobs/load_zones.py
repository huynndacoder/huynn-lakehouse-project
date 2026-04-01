from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("LoadTaxiZones") \
    .config("spark.cores.max", "1") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.lakehouse.type", "hadoop") \
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://datalake/warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin12345") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Create table if it doesn't exist
spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.taxi_zones (
        LocationID INT,
        Borough STRING,
        Zone STRING,
        service_zone STRING
    ) USING iceberg
""")

# Read the CSV you provided
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://raw-data/taxi_zone_lookup.csv")

df.write.format("iceberg").mode("overwrite").saveAsTable("lakehouse.taxi_zones")
print("Taxi Zones successfully loaded to Iceberg!")
spark.stop()