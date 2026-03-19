# screamingfrog

Python library for working with Screaming Frog SEO Spider crawl data via CSV exports,
SQLite databases, Derby-based `.dbseospider` files, and `.seospider` crawl files.

See `methods.md` for a complete method-level API reference.

## Phase 1 status
- Backend interface + Internal-only CSV/DB backends
- Unified `Crawl` loader
- Schema discovery utilities (CSV + SQLite)
- Derby-backed .dbseospider support (requires Java runtime; Derby jars are bundled)

## Quick start

```python
from screamingfrog import Crawl

crawl = Crawl.load("./exports")
for page in crawl.internal.filter(status_code=404):
    print(page.address)
```

## Loading crawl files

```python
from screamingfrog import Crawl, list_crawls

# CSV exports directory
crawl = Crawl.load("./exports")

# SQLite database
crawl = Crawl.load("./crawl.db")

# DuckDB analytics cache
crawl = Crawl.load("./crawl.duckdb")

# Derby .dbseospider file
crawl = Crawl.load("./crawl.dbseospider")

# Screaming Frog .seospider crawl (default: convert to DB + Derby backend)
crawl = Crawl.load("./crawl.seospider")

# Disable .dbseospider materialization (still uses Derby from ProjectInstanceData)
crawl = Crawl.load(
    "./crawl.seospider",
    materialize_dbseospider=False,
)

# Force CSV mode for .seospider (CLI export -> CSV backend)
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_dir="./exports_from_seospider",
    export_tabs=["Internal:All", "External:All", "Response Codes:All"],
)

# Kitchen-sink export profile (all tabs/bulk exports from SF UI)
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_dir="./exports_kitchen",
    export_profile="kitchen_sink",
)

# DB crawl ID (DB mode) loads Derby by default
crawl = Crawl.load("138edb21-61d0-41cd-9e9b-725b592a471c", source_type="db_id")

# DB crawl ID -> export and load a DuckDB analytics cache directly
crawl = Crawl.load(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    source_type="db_id",
    db_id_backend="duckdb",
    duckdb_path="./crawl.duckdb",
    duckdb_tabs="all",
)

# Discover available DB crawls, then load one by ID
latest = list_crawls()[0]
crawl = Crawl.load(latest.db_id, source_type="db_id")
```

### Loader notes
- `.seospider` defaults to DB conversion (CLI load + Derby backend). Use `seospider_backend="csv"` for exports.
- `.seospider` auto-materializes a `.dbseospider` file next to the crawl (overwrite default).
- Set `materialize_dbseospider=False` to avoid creating the `.dbseospider` cache file.
- Set `dbseospider_overwrite=False` to reuse an existing `.dbseospider` cache.
- DB conversion can temporarily set `storage.mode=DB` in `spider.config` (set `ensure_db_mode=False` to skip).
- Internal DB crawl directories (e.g. `ProjectInstanceData/.../results_.../sql`) load via Derby.
- DB crawl IDs load Derby by default; set `db_id_backend="csv"` to force CSV exports.
- DB crawl IDs can export-and-load DuckDB directly with `db_id_backend="duckdb"` and `duckdb_path=...`.
- Set `SCREAMINGFROG_CLI` if the CLI executable is not in a standard install path.
- CLI exports default to the `Internal:All` tab unless `export_tabs` is provided.
- `export_profile="kitchen_sink"` uses bundled export lists captured from the SF UI.
- Derby loads can auto-fallback to CSV exports for missing columns or GUI filters (`csv_fallback=True`, `csv_fallback_profile="kitchen_sink"`).
- CSV fallback cache defaults to `csv_fallback_cache_dir` (next to the crawl); set `csv_fallback=False` to disable.
- `.duckdb` loads use the DuckDB analytics backend and are best for repeated scan-heavy analysis once a cache exists.

## DuckDB analytics cache

Use DuckDB as an optional fast analytics layer on top of Derby crawls:

```python
from screamingfrog import Crawl

derby_crawl = Crawl.load("./crawl.dbseospider", csv_fallback=False)
derby_crawl.export_duckdb("./crawl.duckdb")

fast = Crawl.load("./crawl.duckdb")

pages_404 = fast.pages().filter(status_code=404).collect()
links = fast.links("in").filter(status_code=404).collect()
rows = (
    fast.query("APP", "URLS")
    .select("ENCODED_URL", "RESPONSE_CODE")
    .where("RESPONSE_CODE >= ?", 400)
    .collect()
)
```

Notes:
- Derby remains the source-of-truth crawl store.
- DuckDB is the fast analytics cache for repeated analysis.
- Current DuckDB export materializes key tabs (`internal_all`, `all_inlinks`, `all_outlinks`, redirect/canonical chain tabs) plus raw `APP.URLS`, `APP.LINKS`, and `APP.UNIQUE_URLS`.
- You can also export directly from a DB crawl id with `export_duckdb_from_db_id(...)`.
- `.seospider` and DB crawl ID loaders can export and load DuckDB directly via `backend="duckdb"` / `db_id_backend="duckdb"`.
- Use `tabs="all"` if you want to materialize every currently available mapped tab into the DuckDB cache.

### Discover DB crawls (`list_crawls`)

Use `list_crawls()` to enumerate DB-mode crawls in your local Screaming Frog
`ProjectInstanceData` directory, without opening Derby or starting Java.

```python
from screamingfrog import list_crawls

for info in list_crawls():
    print(info.db_id, info.url, info.urls_crawled, info.modified)
```

`list_crawls(project_root=...)` returns `CrawlInfo` objects with:
- `db_id`: crawl UUID folder name
- `url`: crawl start URL
- `urls_crawled`: number of crawled URLs
- `percent_complete`: crawl completion percentage
- `modified`: last modified timestamp (UTC)
- `path`: absolute path to the crawl folder

## Generic tab access

In addition to the typed `internal` view, you can iterate any exported tab:

```python
from screamingfrog import Crawl

crawl = Crawl.load("./exports")

# List available CSV tabs
print(crawl.tabs)

# Access a tab by file name (extension optional)
for row in crawl.tab("response_codes_all"):
    print(row["Address"], row["Status Code"])

# Filter using column names or snake_case equivalents
for row in crawl.tab("internal_all").filter(status_code="404"):
    print(row["Address"])

# Apply GUI filters (when supported)
for row in crawl.tab("page_titles").filter(gui="Missing"):
    print(row["Address"], row["Title 1"])
```

Notes:
- CSV backend exposes any `*.csv` in the export folder.
- Derby backend exposes tabs mapped in `schemas/mapping.json` (or `SCREAMINGFROG_MAPPING`).
- Hybrid Derby+CSV fallback is enabled by default for `Crawl.load` and will export missing tabs on demand.
- SQLite backend supports only a small set of high-value tabs (response codes, titles, meta description, internal_all).
- For exact GUI filter behavior, use CSV exports (e.g., `export_profile="kitchen_sink"`).
- Derby now natively supports `Response Codes > Internal Redirect Chain` and `Hreflang > Not Using Canonical`.
- HTTP canonical/rel fields in Derby are parsed from `HTTP_RESPONSE_HEADER_COLLECTION` when present.
- Derby-backed `crawl.internal` now materializes computed mapped fields like `Indexability` and `Indexability Status`.
- Derby filters now work against mapped expression fields and header-derived fields in both `crawl.internal` and `crawl.tab(...)`.
- Some link metrics (Link Score, % of Total, JS outlink counts) are not mapped in Derby yet.

## Ergonomic sitewide views

Use first-class page/link views when you do not want to remember tab names:

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

pages_404 = crawl.pages().filter(status_code=404).collect()
nofollow_inlinks = crawl.links("in").filter(rel="nofollow").collect()
blog_pages = crawl.section("/blog").pages().collect()
blog_outlinks = crawl.section("/blog").links("out").collect()
```

Notes:
- `crawl.pages()` is a mapped sitewide page view backed by `internal_all`.
- `crawl.links("in")` / `crawl.links("out")` are sitewide mapped link views backed by `all_inlinks` / `all_outlinks`.
- `crawl.section("/blog")` matches by URL path prefix; pass a full URL prefix if you want host-specific scoping.

## Inlinks / Outlinks (Derby)

When using a `.dbseospider` crawl, you can read inlinks/outlinks directly from Derby:

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

for link in crawl.inlinks("https://example.com/page"):
    if link.data.get("NoFollow"):
        print(link.source, "->", link.destination, link.data.get("Rel"))
```

## Chain helpers (redirect/canonical)

Dedicated chain helpers are available on `Crawl`:

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

# Redirect chains with 3+ hops and no loop
for row in crawl.redirect_chains(min_hops=3, loop=False):
    print(row["Address"], row.get("Number of Redirects"))

# Canonical chains
for row in crawl.canonical_chains(min_hops=2):
    print(row["Address"], row.get("Number of Canonicals"))

# Mixed redirect+canonical chains
for row in crawl.redirect_and_canonical_chains(min_hops=4):
    print(row["Address"], row.get("Number of Redirects/Canonicals"))
```

## Audit helpers

Thin report helpers are available for common workflows:

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

broken = crawl.broken_links_report()
title_meta = crawl.title_meta_audit()
non_indexable = crawl.indexability_audit()
chains = crawl.redirect_chain_report(min_hops=3)
```

Notes:
- `broken_links_report()` returns broken internal URLs with inlink counts and sampled inlink sources when available.
- `title_meta_audit()` currently surfaces missing titles and missing meta descriptions as flat issue rows.
- `indexability_audit()` returns non-indexable pages with the key indexability fields that explain why.
- `redirect_chain_report()` is a collected helper over `crawl.redirect_chains(...)`.

## Escape hatches (raw SQL)

Mapped fields are stable and documented. Raw access is available for advanced users
who want immediate access to Derby/SQLite columns even when mappings are incomplete.

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider", csv_fallback=False)

# Raw table rows (Derby/SQLite only)
for row in crawl.raw("APP.URLS"):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"])

# SQL passthrough (Derby/SQLite only)
for row in crawl.sql(
    "SELECT ENCODED_URL, RESPONSE_CODE FROM APP.URLS WHERE RESPONSE_CODE >= ?",
    [400],
):
    print(row)
```

Notes:
- `raw()` / `sql()` are not supported for CSV/CLI export backends.
- Raw column names may vary by backend and Screaming Frog version.

## Query builder (chainable SQL)

Use a chainable API for common SQL without writing full query strings:

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider", csv_fallback=False)

rows = (
    crawl.query("APP", "URLS")
    .select("ENCODED_URL", "RESPONSE_CODE", "TITLE_1")
    .where("RESPONSE_CODE >= ?", 400)
    .order_by("RESPONSE_CODE DESC", "ENCODED_URL ASC")
    .limit(100)
    .collect()
)
```

Notes:
- `crawl.query(...)` uses the backend SQL engine (Derby/Hybrid/SQLite).
- CSV/CLI export backends do not support SQL/query execution.
- Use `.to_sql()` if you want to inspect the generated SQL + params.
- `InternalView`, `TabView`, `LinkView`, `QueryView`, and `CrawlDiff` also support `to_pandas()` / `to_polars()` with optional dependencies installed.

## Crawl diff (crawl-over-crawl)

```python
from screamingfrog import Crawl

old = Crawl.load("./crawl-2024-01.dbseospider")
new = Crawl.load("./crawl-2024-02.dbseospider")

diff = new.compare(old)

print(diff.summary())

for change in diff.status_changes[:5]:
    print(change.url, change.old_status, "->", change.new_status)
```

Notes:
- Title comparison uses `Title 1` by default (override via `compare(..., title_fields=...)`).
- Redirect changes are best-effort and depend on available columns/headers.
- Additional field changes are captured for canonical + canonical status, meta description/keywords/refresh, H1/H2/H3, word count, indexability, and robots + directives summary by default (override via `compare(..., field_groups=...)`).
- `diff.to_rows()` flattens all change buckets into one row list for export/dataframes.

## Examples

Ready-to-run scripts are available in `examples/`:
- `examples/broken_links_report.py`
- `examples/title_meta_audit.py`
- `examples/crawl_diff.py`

### Tab metadata helpers

```python
from screamingfrog import Crawl

crawl = Crawl.load("./exports")

# List GUI filter names for a tab
print(crawl.tab_filters("Page Titles"))

# Inspect columns (CSV header or Derby mapping)
print(crawl.tab_columns("page_titles"))

# Get both in one shot
print(crawl.describe_tab("page_titles"))
```

## Export profiles

You can access the bundled kitchen-sink export lists directly:

```python
from screamingfrog.config import get_export_profile

profile = get_export_profile("kitchen_sink")
print(len(profile.export_tabs), len(profile.bulk_exports))
```

## CLI wrapper (start crawls + exports)

The package includes Python wrappers around the Screaming Frog CLI:

```python
from screamingfrog import export_crawl, start_crawl

# Start a crawl from a URL
start_crawl(
    "https://example.com",
    "./out",
    save_crawl=True,
    export_tabs=["Internal:All", "Response Codes:All"],
)

# Export from an existing crawl file (.seospider / .dbseospider)
export_crawl(
    "./crawl.seospider",
    "./exports",
    export_tabs=["Internal:All", "Page Titles:Missing"],
)
```

## Packaging .dbseospider files

`.dbseospider` files are zip archives of a DB-mode crawl folder. You can pack or
unpack them with helpers:

```python
from screamingfrog import (
    export_dbseospider_from_seospider,
    pack_dbseospider,
    pack_dbseospider_from_db_id,
    unpack_dbseospider,
)

# Package an internal DB crawl folder
dbseospider = pack_dbseospider(
    r"C:\Users\Antonio\.ScreamingFrogSEOSpider\ProjectInstanceData\<project_id>",
    r"C:\Users\Antonio\my-crawl.dbseospider",
)

# Package by DB crawl ID
dbseospider = pack_dbseospider_from_db_id(
    "7c356a1b-ea14-40f3-b504-36c3046432a2",
    r"C:\Users\Antonio\my-crawl.dbseospider",
)

# Convert a .seospider crawl into .dbseospider
dbseospider = export_dbseospider_from_seospider(
    r"C:\Users\Antonio\schema-discovery\actionnetwork_crawl\crawl.seospider",
    r"C:\Users\Antonio\actionnetwork.dbseospider",
)

# Extract a .dbseospider file
unpack_dbseospider(
    r"C:\Users\Antonio\my-crawl.dbseospider",
    r"C:\Users\Antonio\unpacked_crawl",
)
```

Notes:
- `export_dbseospider_from_seospider` runs the Screaming Frog CLI, then packages
  the newly created DB crawl folder. If your DB storage path is custom, set
  `SCREAMINGFROG_PROJECT_DIR` or pass `project_root=...`.
- The helper can force `storage.mode=DB` via `spider.config` (set `ensure_db_mode=False` to skip).

## Config patches (Custom Search + Custom JavaScript)

Use `ConfigPatches` to build patch JSON for the Java ConfigBuilder:

```python
from screamingfrog import ConfigPatches, CustomSearch, CustomJavaScript

patches = ConfigPatches()
patches.set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")
patches.add_custom_search(CustomSearch(name="Filter 1", query=".*", data_type="REGEX"))
patches.add_custom_javascript(
    CustomJavaScript(name="Extractor 1", javascript="return document.title;")
)

patch_json = patches.to_json()
```

Apply patches directly to a `.seospiderconfig` file:

```python
from screamingfrog import ConfigPatches, write_seospider_config

patches = ConfigPatches().set("mCrawlConfig.mMaxUrls", 5000)

write_seospider_config(
    "base.seospiderconfig",
    "alpha.seospiderconfig",
    patches,
)
```

## Installation (single install)

Derby support (`.dbseospider`) and `.seospiderconfig` writing are included in the base install:

```bash
python -m pip install -e .
```

Optional extras still exist (`[derby]`, `[config]`, `[alpha]`) but are no longer required.

Bundled Derby jars are included with this package (Apache Derby 10.17.1.0), so
`DERBY_JAR` is optional. Set `DERBY_JAR` if you want to override the bundled jars
or use a different Derby install.

### Java runtime setup (for .dbseospider)

The Derby driver jars are bundled, but you still need a Java runtime (`java.exe` / `java`) available.

If Java is missing, Derby loads raise:

`RuntimeError: Java runtime not found. Set JAVA_HOME or add java to PATH.`

Quick checks and fixes:

```bash
java -version
```

- If Screaming Frog desktop is installed, this library already tries these paths automatically:
  - `C:\Program Files (x86)\Screaming Frog SEO Spider\jre`
  - `C:\Program Files\Screaming Frog SEO Spider\jre`
- Otherwise install a JRE/JDK and set `JAVA_HOME` (or add Java to `PATH`).

Windows PowerShell example:

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-21"
$env:Path = "$env:JAVA_HOME\\bin;$env:Path"
```

Third-party notices for Apache Derby are included in `screamingfrog/vendor/derby/NOTICE`.

Derby tab mapping uses `schemas/mapping.json`. Set `SCREAMINGFROG_MAPPING` if
you store the mapping elsewhere.

## Contributing: tab/column mapping

To help map more GUI tabs to Derby (see [Antonio's LinkedIn](https://www.linkedin.com/in/antoniomaculus/) for progress):

- **Source of truth:** `schemas/mapping.json` (keys = normalized export filenames, e.g. `internal_all.csv`).
- **Workflow:** Compare CSV schema in `schemas/csv/` with Derby schema in `schemas/db/tables/`; prefer `db_column` -> `db_expression` -> `header_extract` / `blob_extract` / `derived_extract` / `multi_row_extract` -> `NULL`; then add/update tests.
- **Automation:** Run from repo root:
  ```bash
  python scripts/suggest_mappings.py --tab hreflang_all.csv   # suggestions for one tab
  python scripts/suggest_mappings.py --tab-family hreflang   # all hreflang_* tabs
  python scripts/suggest_mappings.py --list-unmapped          # tabs with unmapped columns
  python scripts/suggest_mappings.py --patch --tab my_tab    # JSON fragment to merge into mapping.json
  python scripts/suggest_mappings.py --report-nulls          # regenerate mapping_nulls.md content
  ```
- **PRs:** Prefer PRs to `schemas/mapping.json` for new column coverage; for repeated Derby SQL incompatibilities, fix in `screamingfrog/backends/derby_backend.py`; for GUI filter parity, use `screamingfrog/filters/*.py`. See `scripts/README.md`, `schemas/mapping_nulls.md`, `schemas/inlinks_mapping_nulls.md`, and `MAPPING_BACKLOG.md` for current backlog and known hard families.

## Development

```bash
python -m pip install -e .[dev]
pytest
```

Optional live smoke coverage for a real local SF crawl:

```bash
SCREAMINGFROG_RUN_LIVE_SMOKE=1 pytest -q tests/test_live_smoke.py -rs --basetemp .pytest-tmp
```
