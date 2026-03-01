"""Post 14: GUI filter access - same filters as the Screaming Frog UI"""
# carbon.sh caption: "Every GUI filter. Now in Python."

from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

# Same filters you'd click in the Screaming Frog GUI
checks = [
    ("page_titles",      "Missing",            "Missing titles"),
    ("page_titles",      "Duplicate",          "Duplicate titles"),
    ("page_titles",      "Over 60 Characters", "Long titles"),
    ("meta_description", "Missing",            "Missing meta desc"),
    ("meta_description", "Duplicate",          "Duplicate meta desc"),
    ("h1",               "Missing",            "Missing H1"),
    ("h1",               "Duplicate",          "Duplicate H1"),
    ("images",           "Missing Alt Text",   "Missing alt text"),
]

for tab, gui, label in checks:
    n = crawl.tab(tab).filter(gui=gui).count()
    print(f"  {label:<25} {n:>5}")
