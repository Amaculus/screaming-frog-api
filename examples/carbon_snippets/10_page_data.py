"""Post 10: Deep page inspection - show all data available per page"""
# carbon.sh caption: "Every field Screaming Frog captures. Accessible as a Python dict."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

page = next(iter(crawl.internal.filter(status_code=200)))

print(f"URL: {page.address}")
print(f"Status: {page.status_code}\n")

for key, value in list(page.data.items())[:15]:
    if value:
        print(f"  {key}: {value}")
