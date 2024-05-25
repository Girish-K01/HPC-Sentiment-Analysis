from gnews import GNews

# Code has been replaced with new Parallelized code

"""
Fetches complete news articles related to a specific topic using the GNews API and the Newspaper3k library.

Parameters:
    topic (str): The topic or keyword for which news articles are to be fetched.
    count (int): The number of news articles to fetch.

Returns:
    list: A list of dictionaries containing the title and full text content of the fetched news articles.
    
Example:
    To fetch 5 news articles related to "AMZN":
    >>> Fetch("AMZN", 5)
"""


def Fetch(topic, count):

    # Initialize GNews API to fetch news from the past week
    News = GNews(language="en", period="7d", max_results=30)

    # Fetch news articles using GNews API
    raw_articles = News.get_news(topic)

    # Initialize an empty list to store processed articles
    processed_articles = []

    # Initialize a counter for fetched articles
    local_count = 0

    # Iterate through each raw article
    for i in raw_articles:
        # Get the full content of the article using its URL
        full_article = News.get_full_article(i["url"])

        # If full article content is not available, skip to the next article
        if full_article is None:
            raw_articles.remove(i)
        else:
            # Increment the local count and store the title and full content in a dictionary
            local_count += 1
            values = {"title": i["title"], "article": full_article.text}
            processed_articles.append(values)

        # If the desired count of articles is fetched, return the processed articles
        if local_count == count:
            return processed_articles
