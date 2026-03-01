"""Post 11: Format conversion pipeline"""
# carbon.sh caption: ".seospider in, .dbseospider out. No GUI required."

from screamingfrog import (
    export_dbseospider_from_seospider,
    pack_dbseospider_from_db_id,
    unpack_dbseospider,
    Crawl,
)

# Convert CLI crawl to portable GUI format
export_dbseospider_from_seospider(
    "./crawl.seospider",
    "./crawl.dbseospider"
)

# Now anyone can open it - in Python OR Screaming Frog GUI
crawl = Crawl.load("./crawl.dbseospider")
print(f"Loaded {crawl.internal.count()} pages")
print("Works without Screaming Frog installed.")
