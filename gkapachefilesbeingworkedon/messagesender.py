from gnews import GNews
from Article_Fetcher import Fetch

# Initialize GNews API
News = GNews(language="en", period="7d", max_results=100)

def send_message(topic):
    articles = Fetch("AMZN", 5)

    # Send each article as a message to Kafka
    for article in articles:
        message = {
            "title": article["title"],
            "article": article["article"]
        }
        producer.send(topic, message)
        producer.flush()
        print(f"Message sent: {message}")

# Example usage
send_message("sentiment-analysis-topic")