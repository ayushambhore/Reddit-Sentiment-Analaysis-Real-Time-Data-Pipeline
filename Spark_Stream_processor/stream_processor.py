from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, from_unixtime, avg, current_timestamp
from pyspark.sql.types import StringType, StructType, StructField, FloatType, TimestampType
import uuid
from datetime import datetime, timedelta

# Function to convert UTC timestamp to IST
def convert_utc_to_ist(utc_timestamp):
    if utc_timestamp is None:
        return None
        
    ist_time = datetime.utcfromtimestamp(utc_timestamp) + timedelta(hours=5, minutes=30)
    return ist_time

convert_ist_udf = udf(convert_utc_to_ist, TimestampType())

# Initialize sentiment analyzer
sentiment_analyzer = SentimentIntensityAnalyzer()

# Function for sentiment analysis
def get_sentiment_score(text):
    if text is None or text.strip() == "":
        return None
    sentiment_score = sentiment_analyzer.polarity_scores(text)
    return sentiment_score['compound']

sentiment_score_udf = udf(get_sentiment_score, FloatType())

# Function to generate unique UUIDs
def generate_uuid():
    return str(uuid.uuid4())

uuid_generator_udf = udf(generate_uuid, StringType())

# Define schema for Kafka message
comment_schema = StructType([
    StructField("id", StringType(), False),
    StructField("body", StringType(), True),
    StructField("subreddit", StringType(), False),
    StructField("timestamp", FloatType(), False)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Reddit Stream Processor") \
    .master("local[*]") \
    .config("spark.jars",
            "/home/sunbeam/.local/lib/python3.10/site-packages/pyspark/jars/spark-cassandra-connector-assembly_2.12-3.5.0.jar")\
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()

# Kafka configurations
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "redditcomments"

# Read data from Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()
    
df_stream.printSchema()

# Parse JSON messages
parsed_stream = df_stream.withColumn("comment_json", from_json(df_stream["value"].cast("string"), comment_schema))

parsed_stream.printSchema()

# Extract necessary fields
processed_data = parsed_stream.select(
    "comment_json.id",
    "comment_json.body",
    "comment_json.subreddit",
    "comment_json.timestamp"
).filter(col("id").isNotNull()) \
    .withColumn("uuid", uuid_generator_udf()) \
    .withColumn("api_timestamp", from_unixtime(col("timestamp"))) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .drop("timestamp") \
    .withColumn("sentiment_score", sentiment_score_udf(col("body")))

    
processed_data.printSchema()

# Write to Cassandra
data_writer = processed_data.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("failOnDataLoss", "false") \
    .options(table="comments", keyspace="reddit") \
    .option("checkpointLocation", "/tmp/check_point/") \
    .outputMode("append") \
    .start()
    

# Compute sentiment averages
sentiment_summary = processed_data \
    .withWatermark("ingest_timestamp", "1 minute") \
    .groupBy("subreddit") \
    .agg(avg("sentiment_score").alias("sentiment_score_avg")) \
    .withColumn("uuid", uuid_generator_udf()) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("ingest_timestamp", convert_ist_udf(col("ingest_timestamp").cast("long")))
    
sentiment_summary.printSchema()

# Write sentiment averages to Cassandra
summary_writer = sentiment_summary.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(
    lambda batchDF, batchID: batchDF.write \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/check_point/") \
        .options(table="subreddit_sentiment_avg", keyspace="reddit") \
        .mode("append") \
        .save()
    ).outputMode("update").start()

spark.streams.awaitAnyTermination()
