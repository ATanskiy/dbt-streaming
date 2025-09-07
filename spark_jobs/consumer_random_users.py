from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, year, month, dayofmonth, hour, minute

# Create Spark session with Iceberg and S3A configuration
spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ice.type", "hadoop")\
    .config("spark.sql.catalog.ice.warehouse", "s3a://raw-from-kafka/")\
    .config("spark.sql.defaultCatalog", "ice")\
    .getOrCreate()

# Create Iceberg table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS default.random_users (
  raw_json STRING,
  inserted_at TIMESTAMP,
  year INT,
  month INT,
  day INT,
  hour INT,
  minute INT
)
USING iceberg
PARTITIONED BY (year, month, day, hour, minute)
""")

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:9092") \
    .option("subscribe", "random_users") \
    .option("startingOffsets", "latest") \
    .load()

# Transform Kafka message and add partitioning fields
df_final = df_raw.selectExpr("CAST(value AS STRING) as raw_json") \
    .withColumn("inserted_at", current_timestamp()) \
    .withColumn("year", year("inserted_at")) \
    .withColumn("month", month("inserted_at")) \
    .withColumn("day", dayofmonth("inserted_at")) \
    .withColumn("hour", hour("inserted_at")) \
    .withColumn("minute", minute("inserted_at"))

# Write to Iceberg table
query = df_final.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://raw-from-kafka/_checkpoints/random_users") \
    .toTable("default.random_users")

query.awaitTermination()


#docker exec -it spark_dbt_streaming bash
#spark-submit /opt/spark/jobs/stream_random_users.py