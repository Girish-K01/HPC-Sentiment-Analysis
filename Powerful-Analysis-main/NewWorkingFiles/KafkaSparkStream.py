from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram
import cudf
from cuml.feature_extraction.text import TfidfVectorizer
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import cudf
import torch
import glob
import numpy as np
import os
import shutil

# Define the Kafka consumer parameters
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "new-topic"

# Define the schema for the incoming data

article_schema = StructType([
    StructField("title", StringType(), True),
    StructField("article", StringType(), True)
])

spark = SparkSession \
    .builder \
    .appName("DataIngestionAndPreprocessing") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .master("local[*]") \
    .getOrCreate()

# Read the data from Kafka

kafka_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "new-topic") \
    .option("startingOffsets", "latest") \
    .load()

#kafka_df.selectExpr("CAST(value AS STRING)").printSchema()
# Parse the incoming data and extract the article information

parsed_df = kafka_df \
    .select(from_json(col("value").cast("string"), article_schema).alias("article")) \
    .select("article.title", "article.article")

# Tokenize the text data
tokenizer = Tokenizer(inputCol="article", outputCol="tokens")
tokenized_df = tokenizer.transform(parsed_df)

# Remove stopwords
stopwords_remover = StopWordsRemover(inputCol="tokens", outputCol="cleaned_tokens")
cleaned_df = stopwords_remover.transform(tokenized_df)

# Convert text to feature vectors using TF-IDF on GPU
ngram = NGram(inputCol="cleaned_tokens", outputCol="ngrams", n=2)
ngram_df = ngram.transform(cleaned_df)

'''
query = parsed_df.writeStream \
    .format("console") \
    .option("truncate", True) \
    .start()
query.awaitTermination()
'''

query = parsed_df \
    .writeStream \
    .format("parquet") \
    .option("path", "./parquet_store") \
    .option("checkpointLocation", "./spark_checkpoint") \
    .start()
query.awaitTermination(60)


#from modelcode import load_sentiment_model
# Read the Parquet files as a cuDF DataFrame
tokenizer = AutoTokenizer.from_pretrained("mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")
model = AutoModelForSequenceClassification.from_pretrained("mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis")

# Move the model to GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
parquet_files = glob.glob("./parquetagain/*.parquet")
df = cudf.read_parquet(parquet_files)


def analyze_sentiment(text):
    sentiments = {0: 'negative', 1: 'neutral', 2: 'positive'}
    encoded_input = tokenizer.encode_plus(text, return_tensors='pt', max_length=512, truncation=True, padding='max_length')
    input_ids = encoded_input['input_ids'].to(device)
    attention_mask = encoded_input['attention_mask'].to(device)
    
    with torch.no_grad():
        outputs = model(input_ids, attention_mask=attention_mask)
        logits = outputs[0]
        sentiment_scores = torch.softmax(logits, dim=1).cpu().numpy()[0]
        xxx = max(sentiment_scores)
        inn = np.where(sentiment_scores==xxx)[0][0]

    return sentiments.get(inn)

# Apply the sentiment analysis function to the cuDF DataFrame
noofrows = df.shape[0]
sentiments = []
for i in range(noofrows):
     sentiments.append(analyze_sentiment(df.iloc[i,1]))
df["sentiments"] = sentiments     
# Display the results
print(df)

def remove_folder_contents(folder_path):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")

# Example usage
folder_path = "./spark_checkpoint"
folder_path2="./parquet_store"
remove_folder_contents(folder_path)
remove_folder_contents(folder_path2)

print("done successfully")
