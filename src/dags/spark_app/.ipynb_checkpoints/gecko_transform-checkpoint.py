import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime

def flatten_category(category_df, spark):
    # Category
    category_schema = StructType([
        StructField("page", StringType(), True),
        StructField("category", StringType(), True),
        StructField("last_7_days", IntegerType(), True),
        StructField("market_cap", StringType(), True),
        StructField("num_coins", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("volume_last_day", StringType(), True),
    ])
    empty_rdd = spark.sparkContext.emptyRDD()
    category_flatten = spark.createDataFrame(empty_rdd, category_schema)

    exploded_page = []

    for page in category_df.schema.fieldNames():
        page_df = category_df.select(explode(col(page)).alias("data")) \
                                .withColumn("page", lit(page))

        exploded_page.append(page_df)

    for i in range(len(exploded_page)):
        exploded_page[i] = exploded_page[i].select(
                            col("page"),
                            col("data.category"), 
                            col("data.last_7_days"), 
                            col("data.market_cap"), 
                            col("data.num_coins"), 
                            col("data.url"), 
                            col("data.volume_last_day")
                        )
        category_flatten = category_flatten.union(exploded_page[i])
    return category_flatten

def flatten_category_details(category_details_df, spark):
    # Category details
    details_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("category_name", StringType(), True),
    ])
    category_details_flatten = spark.createDataFrame(spark.sparkContext.emptyRDD(), details_schema)

    exploded_category = []
    for category in category_details_df.schema.fieldNames():
        temp_df = category_details_df.select(explode(category).alias("data")) \
                                        .select(
                                            col("data.symbol"),
                                            col("data.name"),
                                            col("data.url"),
                                        ) \
                                        .withColumn("category_name", lit(category))

        exploded_category.append(temp_df)

    for i in range(len(exploded_category)):
        category_details_flatten = category_details_flatten.union(exploded_category[i])
    return category_details_flatten





