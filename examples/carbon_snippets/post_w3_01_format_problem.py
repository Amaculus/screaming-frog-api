import os, zipfile
from pathlib import Path

os.environ["JAVA_HOME"] = r"C:\Program Files (x86)\Screaming Frog SEO Spider\jre"

import sys
sys.path.insert(0, r"C:\Users\Antonio\screamingfrog")
from screamingfrog import Crawl

crawl_file = r"C:\Users\Antonio\https-www-screamingfrog-co-uk-.dbseospider"

# Step 1: it's just a ZIP
with zipfile.ZipFile(crawl_file) as zf:
    names = zf.namelist()

print(f"A .dbseospider file is a ZIP archive.")
print(f"{len(names)} files inside.\n")

# Step 2: that ZIP contains a Derby database
top = sorted(set(n.split("/")[0] for n in names if "/" in n))
for folder in top:
    count = sum(1 for n in names if n.startswith(folder + "/"))
    print(f"  {folder}/  ({count} files)")

# Step 3: which means you can query it directly
print("\nLoading as a queryable database...")
crawl = Crawl.load(crawl_file)
print(f"  {crawl.internal.count()} URLs")
print(f"  {len(crawl.tabs)} tabs available")
print("\nNo GUI. No CSV exports. No Screaming Frog installed.")
