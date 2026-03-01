"""Post 1: The Hello World - First impression of the library"""
# carbon.sh caption: "Screaming Frog, meet Python."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

for page in crawl.internal.filter(status_code=404):
    print(page.address)

print(f"\n{crawl.internal.filter(status_code=404).count()} broken pages found")
