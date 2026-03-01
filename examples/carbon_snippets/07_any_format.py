"""Post 7: Universal format loading"""
# carbon.sh caption: "CSV, .seospider, .dbseospider, SQLite. One API."

from screamingfrog import Crawl

# Every format. Same API.
crawl = Crawl.load("./exports")             # CSV directory
crawl = Crawl.load("./crawl.db")            # SQLite
crawl = Crawl.load("./crawl.seospider")     # CLI format
crawl = Crawl.load("./crawl.dbseospider")   # Derby archive

# Always the same interface
for page in crawl.internal.filter(status_code=200):
    print(page.address, page.data.get("Title 1"))
