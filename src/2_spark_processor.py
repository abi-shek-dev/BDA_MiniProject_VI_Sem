import os
import pyspark

# 1. The Windows Hadoop Patch
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] = os.environ.get('PATH', '') + ';C:\\hadoop\\bin'

# 2. The Java Override
if 'JAVA_HOME' in os.environ:
    del os.environ['JAVA_HOME']

# 3. Dynamic Version Matching
spark_version = pyspark.__version__
scala_version = "2.13" if spark_version.startswith("4") else "2.12"
kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}"
mongo_package = f"org.mongodb.spark:mongo-spark-connector_{scala_version}:10.3.0"

print(f"Booting Spark Version: {spark_version} (Using Scala {scala_version} Connectors)")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lower
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark Session (MongoDB is BACK!)
spark = SparkSession.builder \
    .appName("RealTimeSentimentAnalysis") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/ecommerce_db.sentiment_trends") \
    .config("spark.jars.packages", f"{kafka_package},{mongo_package}") \
    .config("spark.hadoop.io.nativeio.disable", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("review_text", StringType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_reviews") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = parsed_df.withColumn(
    "sentiment_score",
    when(lower(col("review_text")).rlike("great|awesome|fantastic|best|love|good"), 1.0)
    .when(lower(col("review_text")).rlike("terrible|bad|worst|broken|disappointed|hate"), -1.0)
    .otherwise(0.0)
)

print("✅ Spark Engine Ready! Writing live data to MongoDB...")

# Write directly to Mongo!
def write_to_mongo(df, epoch_id):
    df.write.format("mongodb").mode("append").save()

query = processed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", "./spark_checkpoint_mongo") \
    .start()

query.awaitTermination()