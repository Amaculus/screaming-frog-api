import os, sys, time
os.environ["JAVA_HOME"] = r"C:\Program Files (x86)\Screaming Frog SEO Spider\jre"
sys.path.insert(0, r"C:\Users\Antonio\screamingfrog")

from screamingfrog import export_dbseospider_from_seospider, Crawl

print("Converting crawl 1...")
export_dbseospider_from_seospider(
    r"C:\Users\Antonio\hn-crawl-1.seospider",
    r"C:\Users\Antonio\hn-crawl-1.dbseospider",
)

print("Converting crawl 2...")
export_dbseospider_from_seospider(
    r"C:\Users\Antonio\hn-crawl-2.seospider",
    r"C:\Users\Antonio\hn-crawl-2.dbseospider",
)

print("Diffing...\n")
old = Crawl.load(r"C:\Users\Antonio\hn-crawl-1.dbseospider")
new = Crawl.load(r"C:\Users\Antonio\hn-crawl-2.dbseospider")

diff = new.compare(old)

print(f"Added:           {len(diff.added_pages)}")
print(f"Removed:         {len(diff.removed_pages)}")
print(f"Status changes:  {len(diff.status_changes)}")
print(f"Title changes:   {len(diff.title_changes)}")
print(f"Redirect changes:{len(diff.redirect_changes)}")
print(f"Field changes:   {len(diff.field_changes)}")

if diff.added_pages:
    print("\nNew pages:")
    for url in diff.added_pages[:5]:
        print(f"  {url}")

if diff.title_changes:
    print("\nTitle changes:")
    for c in diff.title_changes[:3]:
        print(f"  {c.url}")
        print(f"    was: {c.old_title}")
        print(f"    now: {c.new_title}")

if diff.status_changes:
    print("\nStatus changes:")
    for c in diff.status_changes[:5]:
        print(f"  {c.url}  {c.old_status} -> {c.new_status}")
