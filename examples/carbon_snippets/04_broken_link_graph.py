"""Post 4: Link graph analysis - find what's linking to broken pages"""
# carbon.sh caption: "Every 404, and every page linking to it. One query."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

for page in crawl.internal.filter(status_code=404):
    inlinks = list(crawl.inlinks(page.address))
    print(f"\n404: {page.address}")
    print(f"  {len(inlinks)} pages link here:")
    for link in inlinks[:3]:
        print(f"    <- {link.source}")
    if len(inlinks) > 3:
        print(f"    ... and {len(inlinks) - 3} more")
