import boto3
from botocore.client import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:19000",  # or your container name if run inside Docker
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1"
)

bucket_name = "raw-from-kafka"

def ensure_bucket_exists():
    existing_buckets = s3.list_buckets()
    if not any(bucket["Name"] == bucket_name for bucket in existing_buckets["Buckets"]):
        s3.create_bucket(Bucket=bucket_name)
        print(f"✅ Created bucket: {bucket_name}")
    else:
        print(f"✅ Bucket already exists: {bucket_name}")

if __name__ == "__main__":
    ensure_bucket_exists()

# from pyspark.sql import SparkSession

# spark = SparkSession.builder \
#     .appName("LocalVSCodeTest") \
#     .master("local[*]") \
#     .getOrCreate()

# df = spark.createDataFrame([(1, "Sasha"), (2, "Kapara")], ["id", "name"])
# df.show()

# spark.stop()