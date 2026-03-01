"""Post 15: The install + first query - great for a "launch" post"""
# carbon.sh caption: "pip install screamingfrog. That's it. That's the post."
# Show terminal commands first, then Python:

# --- Terminal section (show in a separate carbon block or above) ---
# $ pip install screamingfrog[derby]
# $ python

# --- Python section ---
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")
print(f"Pages:   {crawl.internal.count()}")
print(f"200s:    {crawl.internal.filter(status_code=200).count()}")
print(f"404s:    {crawl.internal.filter(status_code=404).count()}")
print(f"Tabs:    {len(crawl.tabs)}")
