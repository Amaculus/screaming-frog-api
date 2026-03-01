"""Post 3: Full SEO audit in ~15 lines"""
# carbon.sh caption: "A complete SEO audit. In Python. From a single file."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

print("=== SEO Audit ===\n")

for code in [200, 301, 404, 500]:
    n = crawl.internal.filter(status_code=code).count()
    if n: print(f"  HTTP {code}: {n} pages")

for label, tab, gui in [
    ("Missing titles",       "page_titles",       "Missing"),
    ("Duplicate titles",     "page_titles",       "Duplicate"),
    ("Missing descriptions", "meta_description",  "Missing"),
    ("Missing alt text",     "images",            "Missing Alt Text"),
]:
    n = crawl.tab(tab).filter(gui=gui).count()
    print(f"  {label}: {n}")
