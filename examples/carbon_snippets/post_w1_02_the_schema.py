import os, jaydebeapi
from pathlib import Path

os.environ["JAVA_HOME"] = r"C:\Program Files (x86)\Screaming Frog SEO Spider\jre"

db_root = next(Path("./unpacked").rglob("service.properties")).parent

conn = jaydebeapi.connect(
    "org.apache.derby.iapi.jdbc.AutoloadedDriver",
    f"jdbc:derby:{db_root};create=false",
    jars=["derby.jar", "derbyshared.jar"],
)

cursor = conn.cursor()
cursor.execute(
    "SELECT TABLENAME FROM SYS.SYSTABLES "
    "WHERE TABLETYPE='T' ORDER BY TABLENAME"
)
tables = [row[0] for row in cursor.fetchall()]
print(f"{len(tables)} tables found\n")

for t in tables:
    cursor.execute(f"SELECT * FROM APP.{t} FETCH FIRST 1 ROWS ONLY")
    cols = len(cursor.description)
    cursor.execute(f"SELECT COUNT(*) FROM APP.{t}")
    rows = cursor.fetchone()[0]
    if rows > 0:
        print(f"  APP.{t:<40} {cols:>4} cols {rows:>7} rows")
