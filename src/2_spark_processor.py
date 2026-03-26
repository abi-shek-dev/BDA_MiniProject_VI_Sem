import os
import pyspark

# 1. The Windows Hadoop Patch
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
# NEW: Add Hadoop to the system PATH temporarily so Java can see the DLLs
os.environ['PATH'] = os.environ.get('PATH', '') + ';C:\\hadoop\\bin'

# 2. The Java Override
if 'JAVA_HOME' in os.environ:
    del os.environ['JAVA_HOME']

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# 3. Dynamic Version Matching
spark_version = pyspark.__version__
scala_version = "2.13" if spark_version.startswith("4") else "2.12"

kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
mongo_package = f"org.mongodb.spark:mongo-spark-connector_{scala_version}:10.3.0"

print(f"Booting Spark Version: {spark_version} (Using Scala {scala_version} Connectors)")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("RealTimeSentimentAnalysis") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce_db.sentiment_trends") \
    .config("spark.jars.packages", f"{kafka_package},{mongo_package}") \
    .config("spark.hadoop.io.nativeio.disable", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define the schema
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

# Parse the JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Analyze sentiment
analyzer = SentimentIntensityAnalyzer()
def get_sentiment(text):
    if not text: return 0.0
    return analyzer.polarity_scores(text)['compound']

sentiment_udf = udf(get_sentiment, FloatType())
processed_df = parsed_df.withColumn("sentiment_score", sentiment_udf(col("review_text")))

# Write to MongoDB
def write_to_mongo(df, epoch_id):
    df.write.format("mongodb").mode("append").save()

# NEW: Added an explicit local checkpoint location so it stops fighting Windows Temp folders
query = processed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "./spark_checkpoint") \
    .start()

query.awaitTermination()