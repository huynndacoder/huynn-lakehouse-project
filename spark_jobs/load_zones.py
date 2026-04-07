from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LoadTaxiZones").config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog").config("spark.sql.catalog.lakehouse.type", "hadoop").config("spark.sql.catalog.lakehouse.warehouse", "s3a://datalake/warehouse").config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000").config("spark.hadoop.fs.s3a.access.key", "admin").config("spark.hadoop.fs.s3a.secret.key", "admin12345").config("spark.hadoop.fs.s3a.path.style.access", "true").config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem").config("spark.driver.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar").config("spark.executor.extraClassPath", "/opt/spark/jars/hadoop-aws-3.3.4.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar").getOrCreate()

df = spark.read.csv("/tmp/taxi_zone_lookup.csv", header=True, inferSchema=True)
print(f"Loaded {df.count()} zones")

df.writeTo("lakehouse.taxi_zones").createOrReplace()

print("Verifying...")
spark.sql("SELECT COUNT(*) FROM lakehouse.taxi_zones").show()
spark.sql("SELECT * FROM lakehouse.taxi_zones LIMIT 5").show()

print("Taxi zones loaded successfully!")
