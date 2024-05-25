import json
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import tensorflow as tf
import tensorflow_text as tf_text
import tensorflow_hub as hub
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.pipeline import Pipeline
from pyspark.ml import PipelineModel
import cudf
import cuml

# Load the pre-trained BERT model from TensorFlow Hub
bert_preprocess = hub.KerasLayer("https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/3")
bert_encoder = hub.KerasLayer("https://tfhub.dev/tensorflow/bert_en_uncased_L-12_H-768_A-12/4")

# Define the BERT model
text_input = tf.keras.layers.Input(shape=(), dtype=tf.string, name='text')
preprocessed_text = bert_preprocess(text_input)
outputs = bert_encoder(preprocessed_text)

# Neural network layers
n_hidden = 64
dense = tf.keras.layers.Dense(n_hidden, activation='relu')(outputs['pooled_output'])
dropout = tf.keras.layers.Dropout(0.2)(dense)
output = tf.keras.layers.Dense(1, activation='sigmoid', name='sentiment')(dropout)

# Create the BERT model
bert_model = tf.keras.Model(inputs=[text_input], outputs=[output])

# Compile the model
bert_model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])

# Wrap the BERT model for use with Apache Spark
from pyspark.ml.util import MLWritable, MLReadable
from pyspark.ml.wrapper import JavaModel, JavaTransformer

class BERTTransformer(JavaTransformer, MLWritable, MLReadable):
    @classmethod
    def _java_transformer_class(cls):
        return "com.example.BERTTransformer"

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

# Convert Spark DataFrame to cuDF DataFrame
cudf_df = cudf.from_pandas(parsed_df.toPandas())

# Tokenization using TensorFlow Text
tokenizer = tf_text.UnicodeScriptTokenizer()
cudf_df['tokens'] = tokenizer.tokenize(cudf_df['article'])

# Stopword removal using TensorFlow Text
stopwords = tf.constant(['this', 'is', 'a', 'the'])
cudf_df['filtered_tokens'] = tf_text.remove_words(cudf_df['tokens'], stopwords)

# Stemming using TensorFlow Text
stemmer = tf_text.PorterStemmer()
cudf_df['stemmed_tokens'] = stemmer.stem(cudf_df['filtered_tokens'])

# TF-IDF feature extraction using cuML
from cuml.feature_extraction.text import TfidfVectorizer

vectorizer = TfidfVectorizer()
tfidf = vectorizer.fit_transform(cudf_df['stemmed_tokens'])

# Convert cuDF DataFrame back to Spark DataFrame
featurized_df = parsed_df.join(cudf.DataFrame({'stemmed_tokens': cudf_df['stemmed_tokens'], 'tf_idf': tfidf.X}), how='left')

# Create a Spark pipeline
indexer = StringIndexer(inputCol="article", outputCol="label")
assembler = VectorAssembler(inputCols=["tf_idf"], outputCol="text")

# Wrap the BERT model in a Spark transformer
bert_transformer = BERTTransformer(bert_model)

# Define the Spark pipeline
pipeline = Pipeline(stages=[indexer, assembler, bert_transformer])

# Train the pipeline
model = pipeline.fit(featurized_df)

# Save the pipeline model
model.write().overwrite().save("sentiment_analysis_model")

# Start streaming and process data
query = model.transform(featurized_df) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()