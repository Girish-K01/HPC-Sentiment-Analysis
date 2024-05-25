from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split
from pyspark.sql.types import StructType, StructField, StringType

# Define the Kafka consumer parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "test-topic"

# Define the schema for the incoming data
article_schema = StructType([
    StructField("title", StringType(), True),
    StructField("article", StringType(), True)
])

spark = SparkSession \
    .builder \
    .appName("DataIngestionAndPreprocessing") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .master("local[*]") \
    .getOrCreate()

# Read the data from Kafka

kafka_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_df.printSchema()
# Parse the incoming data and extract the article information


parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), article_schema).alias("article")) \
    .select("article.title", "article.article")
    
query = kafka_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()
query.awaitTermination()

