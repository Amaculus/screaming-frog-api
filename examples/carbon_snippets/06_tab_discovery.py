"""Post 6: Tab introspection - discover what data is available"""
# carbon.sh caption: "Every tab. Every filter. Every column. Discoverable."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

print("Available tabs:")
for tab in crawl.tabs[:10]:
    print(f"  {tab}")
print(f"  ... {len(crawl.tabs)} total\n")

info = crawl.describe_tab("page_titles")
print(f"page_titles filters: {info['filters']}")
print(f"page_titles columns: {info['columns'][:5]}")
