
#to test if the api works- conclusion , does not work on windows
from Article_Fetcher import Fetch


articles = Fetch("AMZN", 5)  

# Iterate over the list of fetched articles
for article in articles:
    title = article["title"]
    full_text = article["article"]

    # Print or process the title and full text content as needed
    print("Title:", title)
    print("Full Text:", full_text)
    print("________________________________________________")
    
