from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Initialize Spark Session with MongoDB and Kafka connectors
spark = SparkSession.builder \
    .appName("RealTimeSentimentAnalysis") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/ecommerce_db.sentiment_trends") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Suppress overly verbose Spark logs
spark.sparkContext.setLogLevel("WARN")

# Define the schema of the incoming Kafka JSON data
schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("review_text", StringType(), True)
])

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_reviews") \
    .load()

# Parse the JSON from the Kafka message value
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Create a Python function to analyze sentiment
analyzer = SentimentIntensityAnalyzer()
def get_sentiment(text):
    if not text: return 0.0
    return analyzer.polarity_scores(text)['compound'] # Returns a score from -1 (neg) to 1 (pos)

# Register the function as a Spark UDF (User Defined Function)
sentiment_udf = udf(get_sentiment, FloatType())

# Apply the UDF to our dataframe
processed_df = parsed_df.withColumn("sentiment_score", sentiment_udf(col("review_text")))

# Write the processed stream to MongoDB
def write_to_mongo(df, epoch_id):
    df.write.format("mongo").mode("append").save()

query = processed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .start()

query.awaitTermination()