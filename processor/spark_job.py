import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("KAFKA_TOPIC")
POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
POSTGRES_PROPS = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("CryptoProcessor") \
    .getOrCreate()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price_usd", DoubleType(), True),
    StructField("ts", StringType(), True)
])

print('This has started working')

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value to string
# Parse JSON string to structured data
# Select the fields from the parsed data
parsed = df.selectExpr("CAST(value AS STRING)") \ 
    .select(from_json(col("value"), schema).alias("data")) \ 
    .select("data.*") \ 
    .withColumn("ts", col("ts").cast(TimestampType())) # Convert timestamp string to TimestampType

query = parsed.writeStream \
    .foreachBatch(lambda batch_df, _: batch_df.write.jdbc(POSTGRES_URL, "crypto_prices", mode="append", properties=POSTGRES_PROPS)) \
    .outputMode("append") \
    .start()

query.awaitTermination() # Keeps the stream running
