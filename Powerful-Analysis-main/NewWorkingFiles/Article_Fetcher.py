from gnews import GNews
import multiprocessing


def Fetch_One(article, News):
    """
    Fetches the full content of a single news article using the GNews API.

    Parameters:
        article (dict): A dictionary containing information about the news article, including its URL.
        News (GNews): An instance of the GNews API.

    Returns:
        dict or None: A dictionary containing the title and full text content of the fetched news article, or None if the article content could not be retrieved.
    """
    full_article = News.get_full_article(article["url"].strip())
    if full_article is not None:
        return {"title": article["title"], "article": full_article.text}


def Fetch(topic, count):
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

    # Assuming a 33% failure rate while fetching full article
    num_articles = min(int(count * 1.5), 100)

    # Initialize GNews API to fetch news from the past week
    News = GNews(language="en", period="7d", max_results=num_articles)

    # Fetch news articles using GNews API
    raw_articles = News.get_news(topic)

    # Initialize an empty list to store processed articles
    processed_articles = []

    with multiprocessing.Pool() as pool:
        results = pool.starmap(Fetch_One, [(article, News) for article in raw_articles])

    # If the desired count of articles is fetched, return the processed articles
    for result in results:
        if result and result["article"]:
            processed_articles.append(result)
            if len(processed_articles) == count:
                break

    return processed_articles