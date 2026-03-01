"""Post 8: Batch analysis across multiple crawls"""
# carbon.sh caption: "Audit 100 sites in seconds."

from pathlib import Path
from screamingfrog import Crawl

print(f"{'Site':<30} {'Pages':>6} {'404s':>6} {'Rate':>7}")
print("-" * 53)

for f in sorted(Path("./crawls").glob("*.dbseospider")):
    crawl = Crawl.load(str(f))
    total = crawl.internal.count()
    errors = crawl.internal.filter(status_code=404).count()
    rate = errors / total * 100 if total else 0
    print(f"{f.stem:<30} {total:>6} {errors:>6} {rate:>6.1f}%")
