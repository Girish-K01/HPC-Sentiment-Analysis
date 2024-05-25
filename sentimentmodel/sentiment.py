import torch

# Load model directly
from transformers import AutoTokenizer, AutoModelForSequenceClassification

tokenizer = AutoTokenizer.from_pretrained(
    "mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
)
model = AutoModelForSequenceClassification.from_pretrained(
    "mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
)


# Function to perform sentiment analysis
def predict_sentiment(text):
    inputs = tokenizer(
        text, return_tensors="pt"
    )  # Tokenize and convert to PyTorch tensor
    input_ids = inputs["input_ids"]

    with torch.no_grad():
        outputs = model(input_ids)  # Get model outputs
        logits = outputs.logits  # Extract logits
        predicted_class = torch.argmax(logits, dim=-1)  # Get predicted class

    sentiment_labels = {0: "negative", 1: "neutral", 2: "positive"}
    sentiment = sentiment_labels[
        predicted_class.item()
    ]  # Convert tensor to int and get label
    return sentiment


# Test case
text = "Amazon sales is dropping by 80%"
sentiment = predict_sentiment(text)
print(f"Sentiment: {sentiment}")
