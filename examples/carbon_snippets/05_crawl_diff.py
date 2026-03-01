"""Post 5: Crawl comparison - detect regressions between deploys"""
# carbon.sh caption: "Crawl-over-crawl diffing. Catch SEO regressions before they go live."

from screamingfrog import Crawl

old = Crawl.load("./crawl-before.dbseospider")
new = Crawl.load("./crawl-after.dbseospider")

diff = new.compare(old)

print(f"Added:   {len(diff.added_pages)} pages")
print(f"Removed: {len(diff.removed_pages)} pages")
print(f"Status:  {len(diff.status_changes)} changes")
print(f"Titles:  {len(diff.title_changes)} changes")

print("\nStatus changes:")
for c in diff.status_changes[:5]:
    print(f"  {c.url}  {c.old_status} -> {c.new_status}")
