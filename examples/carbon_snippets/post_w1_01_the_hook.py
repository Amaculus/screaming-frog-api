import os, zipfile, jaydebeapi
from pathlib import Path

os.environ["JAVA_HOME"] = r"C:\Program Files (x86)\Screaming Frog SEO Spider\jre"

# A .dbseospider file is just a ZIP containing an Apache Derby database
with zipfile.ZipFile(r"C:\Users\Antonio\https-www-screamingfrog-co-uk-.dbseospider") as zf:
    zf.extractall("./unpacked")

# Find the Derby database root
db_root = next(Path("./unpacked").rglob("service.properties")).parent

# Connect directly via JDBC
conn = jaydebeapi.connect(
    "org.apache.derby.iapi.jdbc.AutoloadedDriver",
    f"jdbc:derby:{db_root};create=false",
    jars=["derby.jar", "derbyshared.jar"],
)

cursor = conn.cursor()
cursor.execute(
    "SELECT RESPONSE_CODE, ENCODED_URL "
    "FROM APP.URLS WHERE RESPONSE_CODE >= 300 "
    "FETCH FIRST 10 ROWS ONLY"
)

for row in cursor.fetchall():
    print(row[0], row[1])
