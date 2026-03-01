"""Post 13: Redirect chain analysis"""
# carbon.sh caption: "Map every redirect. Find the chains. Fix them."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

print("Redirect Map:\n")
for page in crawl.internal.filter(status_code=301):
    target = page.data.get("Redirect URL") or page.data.get("redirect_url")
    print(f"  {page.address}")
    print(f"    -> {target}\n")

count_301 = crawl.internal.filter(status_code=301).count()
count_302 = crawl.internal.filter(status_code=302).count()
print(f"Total: {count_301} permanent, {count_302} temporary")
