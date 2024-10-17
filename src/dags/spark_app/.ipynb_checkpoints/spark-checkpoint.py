import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
# Minio
from minio import Minio
from io import BytesIO
from minio.error import S3Error
# Utility
import json, sys, os
from gecko_transform import *

# Constant
TODAY = (datetime.date.today()).strftime('%Y-%m-%d')
TWO_DAYS_AGO = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')

# Support function
def connect_to_minio():
    access_key = 'vp821YRUCKiTgGoMEjR6'
    secret_key = 'TkrExNxV2ozU3l9O59nk561fCSONQZSWvrZKoOpK'
    client = Minio(
        endpoint='minio:9000',  # Use the HTTP port
        access_key=access_key,
        secret_key=secret_key,
        secure=False  # Ensure this is False for HTTP
    )
    return client

client = connect_to_minio()

# Create a SparkSession
spark = SparkSession.builder \
    .appName("MinIOIntegration") \
    .config("spark.log.level", "WARN") \
    .config("spark.hadoop.fs.s3a.access.key", "my_account") \
    .config("spark.hadoop.fs.s3a.secret.key", "123456789") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.conf.set('spark.sql.caseSensitive', True)

# Define schema
bars_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("open_time", LongType(), True),
    StructField("symbol_name", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("vol", DoubleType(), True),
    StructField("close_time", LongType(), True),
    StructField("quote_asset_vol", DoubleType(), True),
    StructField("num_trades", IntegerType(), True),
    StructField("taker_base_vol", DoubleType(), True),
    StructField("taker_quote_vol", DoubleType(), True),
    StructField("ignore", IntegerType(), True)
])

# Read from Minio
# Binance
bars_df = spark.read.csv("s3a://binance-bars/*.csv", header=True, inferSchema=True)
latest_prices_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://binance-latest-prices/*.json")
symbols_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://binance-symbols/*.json")

# Gecko
category_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://gecko-category/category.json")
category_details_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://gecko-category-details/category-details.json")


# Transform
symbols_df = symbols_df.withColumn("symbols", explode(col("symbols")))
symbols_df = symbols_df.select(
    col("serverTime"),
    col("symbols.symbol"),
    col("symbols.status"),
    col("symbols.allowTrailingStop"),
    col("symbols.baseAsset"),
    col("symbols.baseAssetPrecision"),
    col("symbols.baseCommissionPrecision")
)

category_df = flattenp_category(category_df)
category_details_df = flatten_category_details(category_details_df, spark)

# # Create bucket if not exists
# spark_bucket = "spark-output"
# if not client.bucket_exists(spark_bucket):
#     client.make_bucket(spark_bucket)

# # Write to Minio
# bars_df.write \
#         .format("csv") \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .save(f"s3a://{spark_bucket}/bars")

# symbols_df.write \
#         .format("csv") \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .save(f"s3a://{spark_bucket}/symbols")

# latest_prices_df.write \
#         .format("csv") \
#         .option("header", "true") \
#         .mode("overwrite") \
#         .save(f"s3a://{spark_bucket}/latest_prices")

# print("Spark run successfully!")
