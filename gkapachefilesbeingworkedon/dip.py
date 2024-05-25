import json
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import tensorflow as tf
import tensorflow_text as tf_text

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Define a function to send messages
def send_message(topic, message):
    producer.send(topic, message)
    producer.flush()
    print(f"Message sent: {message}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SentimentAnalysis") \
    .getOrCreate()

# Create a Kafka source DataFrame
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sentiment-analysis-topic") \
    .load()

# Parse the value as JSON and extract the text field
parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), "title STRING, article STRING").alias("message")) \
    .select("message.title", "message.article")

# Tokenization using TensorFlow Text
tokenizer = tf_text.UnicodeScriptTokenizer()

tokenized_df = parsed_df \
    .withColumn("tokens", tokenizer.tokenize(col("article")))

# Stopword removal using TensorFlow Text
stopwords = tf.constant(['this', 'is', 'a', 'the'])
filtered_df = tokenized_df \
    .withColumn("filtered_tokens", tf_text.remove_words(col("tokens"), stopwords))

# Stemming using TensorFlow Text
stemmer = tf_text.PorterStemmer()
stemmed_df = filtered_df \
    .withColumn("stemmed_tokens", stemmer.stem(col("filtered_tokens")))

# TF-IDF feature extraction
vectorizer = tf.keras.layers.TextVectorization(output_mode='tf_idf')
vectorizer.adapt(stemmed_df.select("stemmed_tokens").collect())

final_df = stemmed_df \
    .withColumn("tf_idf", vectorizer(col("stemmed_tokens")))

# Start streaming and process data
query = final_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()