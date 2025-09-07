from pyspark.sql import SparkSession

# Create Spark session with the same Iceberg + MinIO config
spark = SparkSession.builder \
    .appName("ReadIcebergTable") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ice.type", "hadoop") \
    .config("spark.sql.catalog.ice.warehouse", "s3a://raw-from-kafka/") \
    .config("spark.sql.defaultCatalog", "ice") \
    .getOrCreate()

# Option 1: Read via SQL
print("=== Using Spark SQL ===")
spark.sql("SELECT * FROM default.random_users LIMIT 20").show(truncate=False)

# Option 2: Read via DataFrame API
print("=== Using DataFrame API ===")
df = spark.table("default.random_users")
df.show(2, truncate=False)

spark.stop()


#docker exec -it spark_dbt_streaming bash
#spark-submit /opt/spark/jobs/query_iceberg_table.py