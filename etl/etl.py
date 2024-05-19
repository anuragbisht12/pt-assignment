"""This pyspark scripts contains extraction, transform and load code."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, split, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.avro.functions import from_avro

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("AdvertiseX Data Processing") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schemas
impressions_schema = StructType([
    StructField("ad_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("website", StringType(), True)
])

clicks_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("ad_id", StringType(), True),
    StructField("conversion_type", StringType(), True)
])

bid_requests_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("auction_id", StringType(), True),
    StructField("targeting_criteria", StringType(), True)
])

# Extraction 

# Read ad impressions from Kafka
# apply validations and deduplication
impressions_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ad_impressions") \
    .load() \
    .select(from_json(col("value").cast("string"), impressions_schema).alias("data")) \
    .select("data.*") \
    .filter(col("ad_id").isNotNull() & col("user_id").isNotNull() & col("timestamp").isNotNull() & col("website").isNotNull()) \
    .dropDuplicates(["user_id", "ad_id", "timestamp"]) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp")))

# Read clicks and conversions from Kafka
# apply validations and deduplication
clicks_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "clicks_conversions") \
    .load() \
    .select(expr("CAST(value AS STRING)").alias("value")) \
    .select(split(col("value"), ',').alias("values")) \
    .selectExpr("values[0] as timestamp", "values[1] as user_id", "values[2] as ad_id", "values[3] as conversion_type") \
    .filter(col("timestamp").isNotNull() & col("user_id").isNotNull() & col("ad_id").isNotNull() & col("conversion_type").isNotNull()) \
    .dropDuplicates(["user_id", "ad_id", "timestamp"]) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp")))

# Read bid requests from Kafka
# apply validations and deduplication
bid_requests_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bid_requests") \
    .load() \
    .select(from_avro(col("value"), bid_requests_schema.json()).alias("data")) \
    .select("data.*") \
    .filter(col("user_id").isNotNull() & col("auction_id").isNotNull() & col("targeting_criteria").isNotNull()) \
    .dropDuplicates(["user_id", "auction_id"]) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp")))


# Data transformation and enrichment
enriched_df = impressions_df.join(clicks_df, ["user_id", "ad_id"], "left_outer") \
    .select(impressions_df["*"], clicks_df["conversion_type"]) \
    .join(bid_requests_df, ["user_id"], "left_outer") \
    .select(impressions_df["*"], clicks_df["conversion_type"], bid_requests_df["auction_id"], bid_requests_df["targeting_criteria"]) \
    .withColumn("year", year(col("timestamp"))) \
    .withColumn("month", month(col("timestamp"))) \
    .withColumn("day", dayofmonth(col("timestamp")))

# load
# Write ad impressions to Delta Lake
impressions_query = impressions_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://test-bucket/checkpoint-dir/impressions") \
    .option("path", "s3://test-bucket/processed/impressions") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

# Write clicks and conversions to Delta Lake
clicks_query = clicks_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://test-bucket/checkpoint-dir/clicks") \
    .option("path", "s3://test-bucket/processed/clicks") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

# Write bid requests to Delta Lake
bid_requests_query = bid_requests_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://test-bucket/checkpoint-dir/bid_requests") \
    .option("path", "s3://test-bucket/processed/bid_requests") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

# Write enriched data to Delta Lake
# partition the data for efficient quering
enriched_query = enriched_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://test-bucket/checkpoint-dir/enriched") \
    .option("path", "s3://test-bucket/processed/enriched") \
    .partitionBy("year", "month", "day") \
    .outputMode("append") \
    .start()

impressions_query.awaitTermination()
clicks_query.awaitTermination()
bid_requests_query.awaitTermination()
enriched_query.awaitTermination()
