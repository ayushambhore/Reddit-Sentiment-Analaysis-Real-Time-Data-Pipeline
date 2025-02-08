from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, from_unixtime, avg, current_timestamp, lit
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType, FloatType, LongType, \
    DoubleType, TimestampType
import uuid
from datetime import datetime, timedelta


def utc_to_ist(utc_timestamp):
    if utc_timestamp is None:
        return None
    # Convert timestamp from UTC to IST
    utc_time = datetime.utcfromtimestamp(utc_timestamp)
    ist_time = utc_time + timedelta(hours=5, minutes=30)
    return ist_time


ist_udf = udf(utc_to_ist, TimestampType())

# Initialize sentiment analyzer
analyzer = SentimentIntensityAnalyzer()


# Define UDF for sentiment analysis
def analyze_sentiment(text):
    analyzer = SentimentIntensityAnalyzer()

    # Check if text is None or empty
    if text is None or text.strip() == "":
        return None

    sentiment = analyzer.polarity_scores(text)
    return sentiment['compound']  # Return only the compound score


# Define UDF for sentiment analysis
sentiment_udf = udf(analyze_sentiment, FloatType())


#
# # Define UDF for UUID generation
def make_uuid():
    return str(uuid.uuid1())


uuid_udf = udf(make_uuid, StringType())

# Define the schema for the JSON value column
comment_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("body", StringType(), nullable=True),
    StructField("subreddit", StringType(), nullable=False),
    StructField("timestamp", FloatType(), nullable=False)
])

# Initialize Spark session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Reddit Stream Processor") \
    .master("local[*]") \
    .config("spark.jars", "/home/sunbeam/Desktop/project/jars/*.jar") \
    .config("spark.cassandra.connection.host", "127.0.0.1") \
    .getOrCreate()


# # Kafka configurations
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "redditcomments"
#
# # Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

df.printSchema()
#
# # Parse the value column as JSON
parsed_df = df.withColumn(
    "comment_json",
    from_json(df["value"].cast("string"), comment_schema)
)
parsed_df.printSchema()

#
# # Transform the data
output_df = parsed_df.select(
    "comment_json.id",
    "comment_json.body",
    "comment_json.subreddit",
    "comment_json.timestamp"
) \
    .filter(col('id').isNotNull()) \
    .withColumn("uuid", uuid_udf()) \
    .withColumn("api_timestamp", from_unixtime(col("timestamp"))) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .drop("timestamp") \
    .withColumn(
    'sentiment_score', sentiment_udf(col('body'))
)

output_df.printSchema()
#
# # Write data to Cassandra
output_df.writeStream \
    .option("checkpointLocation", "/tmp/check_point/") \
    .option("failOnDataLoss", "false") \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="comments", keyspace="reddit") \
    .outputMode("append") \
    .start()

# Compute moving averages and write to Cassandra
summary_df = output_df \
    .withWatermark("ingest_timestamp", "1 minute") \
    .groupBy("subreddit") \
    .agg(avg("sentiment_score").alias("sentiment_score_avg")) \
    .withColumn("uuid", uuid_udf()) \
    .withColumn("ingest_timestamp", current_timestamp()) \
    .withColumn("ingest_timestamp", ist_udf(col("ingest_timestamp").cast("long")))

summary_df.printSchema()

summary_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(
    lambda batchDF, batchID: batchDF.write.format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/check_point/") \
        .options(table="subreddit_sentiment_avg", keyspace="reddit") \
        .mode("append").save()
    ).outputMode("update").start()

spark.streams.awaitAnyTermination()
