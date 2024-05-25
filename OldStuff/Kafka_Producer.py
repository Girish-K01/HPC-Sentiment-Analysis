from kafka import KafkaProducer
import json
from Article_Fetcher import Fetch
import time

# Kafka configurations
bootstrap_servers = "localhost:9092"
topic = "news"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# User input for stock symbol and count of articles to fetch
stock = input("Enter the stock symbol: ")
count = int(input("Enter the count of articles to fetch: "))

# Fetch articles based on the stock symbol and count
responses = Fetch(stock, count)

# Convert fetched data to JSON format
data = json.dumps(responses, indent=2)

# Send each fetched article to Kafka topic
for i in data:
    producer.send(topic, value=i)

# Flush and close the producer
producer.flush()
producer.close()
