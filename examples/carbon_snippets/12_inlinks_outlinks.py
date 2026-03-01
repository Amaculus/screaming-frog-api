"""Post 12: Link graph deep dive"""
# carbon.sh caption: "The full link graph. Inlinks, outlinks, anchors, rel attributes."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

url = next(iter(crawl.internal)).address

print(f"Link analysis: {url}\n")

inlinks = list(crawl.inlinks(url))
print(f"Inlinks ({len(inlinks)}):")
for link in inlinks[:5]:
    print(f"  {link.source}")
    print(f"    anchor: {link.anchor_text or '(none)'}")

outlinks = list(crawl.outlinks(url))
print(f"\nOutlinks ({len(outlinks)}):")
for link in outlinks[:5]:
    print(f"  {link.destination}")
