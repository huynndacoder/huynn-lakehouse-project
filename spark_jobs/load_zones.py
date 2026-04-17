from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("LoadTaxiZones")
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
        "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar",
    )
    .getOrCreate()
)

import os

csv_path = os.environ.get("TAXI_ZONES_CSV", "/tmp/taxi_zone_lookup.csv")
if not os.path.exists(csv_path):
    alt_path = "/data/taxi_zone_lookup.csv"
    if os.path.exists(alt_path):
        csv_path = alt_path

if spark.catalog.tableExists("lakehouse.lakehouse.taxi_zones"):
    print("Table 'lakehouse.lakehouse.taxi_zones' already exists. Skipping creation.")
    spark.sql("SELECT COUNT(*) FROM lakehouse.lakehouse.taxi_zones").show()
else:
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    print(f"Loaded {df.count()} zones")

    df.writeTo("lakehouse.lakehouse.taxi_zones").create()

    print("Verifying...")
    spark.sql("SELECT COUNT(*) FROM lakehouse.lakehouse.taxi_zones").show()
    spark.sql("SELECT * FROM lakehouse.lakehouse.taxi_zones LIMIT 5").show()

    print("Taxi zones loaded successfully!")
