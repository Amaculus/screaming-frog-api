"""Post 2: Raw SQL escape hatch - the one from your original image"""
# carbon.sh caption: "SQL queries on Screaming Frog's Derby database. No GUI needed."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider", csv_fallback=False)

for row in crawl.sql(
    "SELECT ENCODED_URL, RESPONSE_CODE, TITLE_1 "
    "FROM APP.URLS WHERE RESPONSE_CODE >= 400"
):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"], row["TITLE_1"])
