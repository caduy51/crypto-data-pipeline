import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
# Minio
from minio import Minio
from io import BytesIO
from minio.error import S3Error
# Utility
import json, sys, os, datetime
from gecko_transform import *
from create_date_dim import create_date_dim
from helpers.my_minio import connect_to_minio
from helpers.credentials import *

def create_spark_session():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Spark with Minio") \
        .config("spark.log.level", "WARN") \
        .config("spark.hadoop.fs.s3a.access.key", minio_user) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_password) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    spark.conf.set('spark.sql.caseSensitive', True)
    print("> Spark session created !")
    return spark

minio_client = connect_to_minio()
spark = create_spark_session()

# Define schema
bars_schema = StructType([
    StructField("date", StringType(), True),
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


########################         Read Files from Minio         ########################
# Binance
print("Reading binance files ...")
bars_df = spark.read.csv("s3a://binance-bars/*.csv", header=True, schema=bars_schema)
symbols_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://binance-symbols/*.json")

bars_df.show(5)

print("Reading Gecko files ...")
# Gecko
category_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://gecko-category/category.json")
category_details_df = spark.read.option("multiline", "true").option("header", "true").json("s3a://gecko-category-details/category-details.json")

# Create fact and 2 dims
order_types_df = symbols_df.withColumn("symbols", explode(col("symbols"))) \
            .select("symbols.symbol", "symbols.orderTypes") \
            .withColumn("orderTypes", explode("orderTypes"))

distinct_order_types_df = order_types_df.select("orderTypes").distinct() \
    .withColumn("order_type_id", monotonically_increasing_id())

distinct_symbols_df = order_types_df.select("symbol").distinct() \
    .withColumn("symbol_id", monotonically_increasing_id())

# join and replace with key
order_types_df = order_types_df.join(distinct_symbols_df, order_types_df.symbol == distinct_symbols_df.symbol , how = "left") \
                            .join(distinct_order_types_df, order_types_df.orderTypes == distinct_order_types_df.orderTypes) \
                            .select("symbol_id", "order_type_id")

### Create date_dim
date_dim_df = spark.createDataFrame(create_date_dim("2024-01-01", "2024-12-31"))


########################            Transform          ####################
category_df = flatten_category(category_df, spark)
category_details_df = flatten_category_details(category_details_df, spark)

symbols_df = symbols_df.withColumn("symbols", explode(col("symbols"))) \
                        .select(
                            col("serverTime"),
                            col("symbols.symbol"),
                            col("symbols.status"),
                            col("symbols.baseAsset")
                        )

symbols_df = symbols_df.join(category_details_df, symbols_df.baseAsset == category_details_df.symbol) \
            .withColumnRenamed("category_name", "categoryName") \
            .drop(category_details_df["name"], category_details_df["url"], category_details_df["symbol"])

# Create bucket if not exists
spark_bucket = "spark-output"
if not minio_client.bucket_exists(spark_bucket):
    minio_client.make_bucket(spark_bucket)

# Write to Minio
# binance
bars_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/binance_bars")
print("Wrote successuflly bars ")

symbols_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/binance_symbols")
print("Wrote successuflly symbols")

# gecko
category_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/category_overview")
print("Wrote successuflly category_overview")

# fact and 2 dims
order_types_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/fact_order_types")
print("Wrote successuflly fact_order_types !")

distinct_order_types_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/dim_order_types")
print("Wrote successuflly dim_order_types !")

distinct_symbols_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/dim_symbols")
print("Wrote successuflly dim_symbol_dim !")

date_dim_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"s3a://{spark_bucket}/dim_date")
print("Wrote successuflly dim_date !")

# Finally
print(">> Spark run successfully! << ")

