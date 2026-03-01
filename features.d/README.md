# Features (Complete Reference)

Last updated: 2026-01-29

This is the comprehensive feature documentation for the **screamingfrog** Python library.
It describes what the library can do, how to use it, and includes detailed examples.

---

## Table of Contents

1. [What This Library Is For](#what-this-library-is-for)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [High-Level Capabilities](#high-level-capabilities)
5. [Core API](#core-api)
6. [Loading Crawl Data](#loading-crawl-data)
7. [Accessing Internal Pages](#accessing-internal-pages)
8. [Generic Tab Access](#generic-tab-access)
9. [GUI Filters](#gui-filters)
10. [Link Graph (Inlinks/Outlinks)](#link-graph-inlinksoutlinks)
11. [Crawl Diff](#crawl-diff)
12. [Raw SQL Access](#raw-sql-access)
13. [CLI Exports](#cli-exports)
14. [Database Packaging](#database-packaging)
15. [Config Patches](#config-patches)
16. [Data Models](#data-models)
17. [Environment Variables](#environment-variables)
18. [Use Cases](#use-cases)
19. [Known Limitations](#known-limitations)

---

## What This Library Is For

This library provides programmatic access to Screaming Frog SEO Spider crawl data from Python,
without requiring the GUI. It supports multiple data sources:

- **CSV exports**: Folders exported from Screaming Frog GUI
- **SQLite databases**: `.db` or `.sqlite` files
- **Derby databases**: `.dbseospider` files (Apache Derby format)
- **Crawl files**: `.seospider` files (loaded via CLI automation)
- **DB-mode crawls**: Internal ProjectInstanceData folders by crawl ID

### Design Goals

- **Prefer `.dbseospider`** for speed, portability, and full feature access
- **Typed internal view** for common URL-level analysis
- **Generic tab API** for CSV-like access to any export tab
- **Link graph** (inlinks/outlinks) without GUI dependency
- **Escape hatch** for raw SQL when mappings are incomplete
- **Crawl diff** for detecting changes between crawls
- **Automation** via CLI export helpers and config patches

---

## Installation

```bash
# Basic installation
pip install screamingfrog

# With Derby support (recommended)
pip install screamingfrog[derby]

# Development installation
pip install -e .[dev]
```

### Requirements

- Python 3.9+
- Java runtime (for Derby backend)
- Screaming Frog CLI (for `.seospider` loading and CLI exports)

---

## Quick Start

```python
from screamingfrog import Crawl

# Load a crawl (auto-detects source type)
crawl = Crawl.load("./crawl.dbseospider")

# Iterate internal pages
for page in crawl.internal:
    print(page.address, page.status_code)

# Filter by status code
for page in crawl.internal.filter(status_code=404):
    print("Broken:", page.address)

# Count pages
print(f"Total internal pages: {crawl.internal.count()}")
```

---

## High-Level Capabilities

| Capability | Description |
|------------|-------------|
| **Unified Loader** | `Crawl.load()` auto-detects CSV, SQLite, Derby, or CLI sources |
| **Typed Internal View** | `InternalPage` dataclass with address, status_code, and full data dict |
| **Generic Tab Access** | Iterate any export tab with `crawl.tab("page_titles")` |
| **GUI Filters** | Apply Screaming Frog UI filters like `filter(gui="Missing")` |
| **Link Graph** | `crawl.inlinks(url)` and `crawl.outlinks(url)` with rich metadata |
| **Crawl Diff** | Compare two crawls for status, title, redirect, and field changes |
| **Raw SQL** | Direct database access with `crawl.sql()` and `crawl.raw()` |
| **CLI Automation** | Export tabs programmatically with kitchen-sink profile |
| **DB Packaging** | Pack/unpack `.dbseospider` files for portability |
| **Config Patches** | Build custom search, JavaScript, and extraction configurations |

---

## Core API

### Primary Entry Point

```python
from screamingfrog import Crawl

# Unified loader (recommended)
crawl = Crawl.load("./path/to/source")

# Alternative constructors
crawl = Crawl.from_exports("./exports")           # CSV exports
crawl = Crawl.from_database("./crawl.db")         # SQLite
crawl = Crawl.from_derby("./crawl.dbseospider")   # Derby
crawl = Crawl.from_seospider("./crawl.seospider") # CLI load
crawl = Crawl.from_db_id("138edb21-...")          # DB crawl ID
```

### Core Methods

| Method | Description |
|--------|-------------|
| `crawl.internal` | Typed internal page view with `filter()` and `count()` |
| `crawl.tab(name)` | Generic tab access returning dict rows |
| `crawl.tabs` | List available tab names |
| `crawl.inlinks(url)` | Get inlinks to a URL (Derby only) |
| `crawl.outlinks(url)` | Get outlinks from a URL (Derby only) |
| `crawl.compare(other)` | Compute crawl diff |
| `crawl.raw(table)` | Raw table iteration (DB only) |
| `crawl.sql(query, params)` | SQL passthrough (DB only) |

### Metadata Helpers

| Method | Description |
|--------|-------------|
| `crawl.tab_filters(name)` | List GUI filter names for a tab |
| `crawl.tab_filter_defs(name)` | Full filter definitions with SQL |
| `crawl.tab_columns(name)` | Column names from CSV or Derby mapping |
| `crawl.describe_tab(name)` | Combined columns + filters dict |

---

## Loading Crawl Data

### Auto-Detection

`Crawl.load(path)` auto-detects the source type:

| Path Pattern | Detection |
|--------------|-----------|
| Directory with `*.csv` | CSV exports backend |
| Directory with `service.properties` | Derby backend |
| File with `.dbseospider` extension | Derby backend |
| File with `.sqlite` or `.db` extension | SQLite backend |
| File with `.seospider` extension | CLI load → Derby |
| UUID-like string | DB crawl ID lookup |

### Loading Examples

```python
from screamingfrog import Crawl

# Auto-detect (recommended)
crawl = Crawl.load("./crawl.dbseospider")

# Force specific loader
crawl = Crawl.load("./data", source_type="csv")
crawl = Crawl.load("./data", source_type="derby")
crawl = Crawl.load("./data", source_type="sqlite")
crawl = Crawl.load("./crawl.seospider", source_type="seospider")
crawl = Crawl.load("138edb21-...", source_type="db_id")
```

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `source_type` | `"auto"` | Force loader: `csv`, `sqlite`, `derby`, `seospider`, `db_id` |
| `seospider_backend` | `"derby"` | Backend for `.seospider`: `derby` or `csv` |
| `materialize_dbseospider` | `True` | Create `.dbseospider` cache for `.seospider` loads |
| `csv_fallback` | `True` | Enable Hybrid fallback to CSV for missing mappings |
| `export_profile` | `None` | Export profile for CLI: `"kitchen_sink"` |

### Advanced Options

```python
# .seospider with full control
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",            # or "csv"
    materialize_dbseospider=True,         # Create .dbseospider cache
    dbseospider_overwrite=True,           # Overwrite existing cache
    ensure_db_mode=True,                  # Force DB storage mode
    csv_fallback=True,                    # Enable Hybrid fallback
    csv_fallback_profile="kitchen_sink",  # Export profile for fallback
)

# Derby with custom mapping
crawl = Crawl.load(
    "./crawl.dbseospider",
    mapping_path="/path/to/custom/mapping.json",
    derby_jar="/path/to/derby.jar",
    csv_fallback=False,  # Disable fallback for pure Derby
)

# CSV exports with CLI automation
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_dir="./exports",
    export_tabs=["Internal:All", "Response Codes:All"],
    export_profile="kitchen_sink",
)
```

---

## Accessing Internal Pages

### InternalView API

```python
# Iterate all internal pages
for page in crawl.internal:
    print(page.address, page.status_code)

# Filter by status code
for page in crawl.internal.filter(status_code=404):
    print("Broken:", page.address)

# Filter by multiple values (OR)
for page in crawl.internal.filter(status_code=[404, 410]):
    print("Gone:", page.address)

# Count pages
total = crawl.internal.count()
errors = crawl.internal.filter(status_code=404).count()
```

### InternalPage Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `address` | `str` | URL of the page |
| `status_code` | `Optional[int]` | HTTP status code |
| `id` | `Optional[int]` | Database ID (Derby/SQLite) |
| `data` | `dict[str, Any]` | Full row data as dictionary |

### Accessing Full Data

```python
for page in crawl.internal:
    # Access mapped columns via data dict
    title = page.data.get("Title 1") or page.data.get("Title")
    meta = page.data.get("Meta Description 1")
    word_count = page.data.get("Word Count")
    indexable = page.data.get("Indexability")

    print(f"{page.address}: {title} ({word_count} words)")
```

---

## Generic Tab Access

### TabView API

```python
# List available tabs
print(crawl.tabs)

# Access a tab by name
for row in crawl.tab("response_codes_all"):
    print(row["Address"], row["Status Code"])

# Tab names are normalized automatically
for row in crawl.tab("Page Titles"):       # Works
for row in crawl.tab("page_titles"):       # Works
for row in crawl.tab("page_titles.csv"):   # Works
for row in crawl.tab("page_titles_all"):   # Works
```

### Column Filtering

```python
# Filter by column value
for row in crawl.tab("internal_all").filter(status_code=404):
    print(row["Address"])

# Multiple filters (AND)
for row in crawl.tab("internal_all").filter(
    status_code=200,
    indexability="Non-Indexable"
):
    print(row["Address"])

# Case-insensitive column names
for row in crawl.tab("internal_all").filter(Status_Code=404):
    print(row["Address"])
```

### GUI Filters

```python
# Apply GUI-style filter
for row in crawl.tab("page_titles").filter(gui="Missing"):
    print("Missing title:", row["Address"])

# Multiple GUI filters
for row in crawl.tab("page_titles").filter(gui_filters=["Missing", "Duplicate"]):
    print(row["Address"])

# List available filters
filters = crawl.tab_filters("Page Titles")
print(filters)  # ["All", "Missing", "Duplicate", "Over X Characters", ...]
```

### Tab Metadata

```python
# Get column names
columns = crawl.tab_columns("page_titles")
print(columns)  # ["Address", "Title 1", "Title 1 Length", ...]

# Get filter definitions
filter_defs = crawl.tab_filter_defs("Page Titles")
for filt in filter_defs:
    print(f"{filt.name}: {filt.description}")

# Combined metadata
metadata = crawl.describe_tab("page_titles")
print(metadata["columns"])
print(metadata["filters"])
```

---

## GUI Filters

The library supports GUI-style filters matching the Screaming Frog UI.

### Response Codes Filters

| Filter | Description |
|--------|-------------|
| `All` | All URLs with a response code |
| `Blocked by Robots.txt` | URLs blocked by robots.txt |
| `Blocked Resource` | Resources blocked by robots.txt |
| `No Response` | No response received |
| `Success (2xx)` | HTTP 2xx responses |
| `Redirection (3xx)` | HTTP 3xx responses |
| `Redirection (JavaScript)` | JavaScript redirects |
| `Redirection (Meta Refresh)` | Meta refresh redirects |
| `Client Error (4xx)` | HTTP 4xx responses |
| `Server Error (5xx)` | HTTP 5xx responses |
| `Internal All` | All internal URLs |
| `Internal Success (2xx)` | Internal 2xx responses |
| `Internal Redirection (3xx)` | Internal 3xx responses |
| `Internal Client Error (4xx)` | Internal 4xx responses |
| `Internal Server Error (5xx)` | Internal 5xx responses |
| `External All` | All external URLs |
| `External Client Error (4xx)` | External 4xx responses |

### Page Titles Filters

| Filter | Description |
|--------|-------------|
| `All` | All page titles |
| `Missing` | Missing title tag |
| `Duplicate` | Duplicate title text |
| `Over X Characters` | Title over 60 characters |
| `Below X Characters` | Title below 30 characters |
| `Over X Pixels` | Title over pixel threshold |
| `Below X Pixels` | Title below pixel threshold |
| `Same as H1` | Title matches H1 |
| `Multiple` | Multiple title tags |
| `Outside <head>` | Title outside `<head>` |

### Meta Description Filters

| Filter | Description |
|--------|-------------|
| `All` | All meta descriptions |
| `Missing` | Missing meta description |
| `Duplicate` | Duplicate description text |
| `Over X Characters` | Over 155 characters |
| `Below X Characters` | Below 70 characters |
| `Multiple` | Multiple meta descriptions |

### Canonicals Filters

| Filter | Description |
|--------|-------------|
| `All` | All canonicals |
| `Canonicalised` | URLs with canonical flag |
| `Missing` | Missing canonical tag |
| `Multiple` | Multiple canonical tags |
| `Multiple Conflicting` | Conflicting canonicals |
| `Self Referencing` | Self-referencing canonical |
| `Contains Canonical` | Contains canonical tag |
| `Outside <head>` | Canonical outside `<head>` |
| `Non-Indexable Canonical` | Target is non-indexable |

### Internal Tab Filters

| Filter | Description |
|--------|-------------|
| `All` | All internal URLs |
| `HTML` | HTML content type |
| `JavaScript` | JavaScript files |
| `CSS` | CSS files |
| `Images` | Image files |
| `PDF` | PDF documents |
| `Other` | Other content types |

---

## Link Graph (Inlinks/Outlinks)

### Basic Usage (Derby Only)

```python
# Get inlinks to a URL
for link in crawl.inlinks("https://example.com/page"):
    print(f"{link.source} -> {link.destination}")
    print(f"  Anchor: {link.anchor_text}")
    print(f"  Rel: {link.data.get('Rel')}")

# Get outlinks from a URL
for link in crawl.outlinks("https://example.com/page"):
    print(f"{link.source} -> {link.destination}")
```

### Link Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `source` | `Optional[str]` | Source URL |
| `destination` | `Optional[str]` | Destination URL |
| `anchor_text` | `Optional[str]` | Link anchor text |
| `data` | `dict[str, Any]` | Full link data |

### Link Data Fields

The `data` dict may contain:

| Field | Description |
|-------|-------------|
| `Rel` | Link rel attribute |
| `NoFollow` | NoFollow flag |
| `Alt Text` | Image alt text |
| `Link Path` | DOM path to link |
| `Link Position` | Position on page |
| `Status Code` | Destination status code |
| `Content Type` | Destination content type |
| `Link Type` | Type label (see below) |

### Link Types

| Type ID | Label |
|---------|-------|
| 1 | Hyperlink |
| 6 | Canonical |
| 8 | Rel Prev |
| 10 | Rel Next |
| 12 | Hreflang (HTTP) |
| 13 | Hreflang |

### Example: Find Inlinks to Broken Pages

```python
# Find all inlinks to 404 pages
for page in crawl.internal.filter(status_code=404):
    print(f"\nBroken: {page.address}")
    for link in crawl.inlinks(page.address):
        print(f"  <- {link.source} ({link.anchor_text})")
```

---

## Crawl Diff

Compare two crawls to detect changes.

### Basic Usage

```python
from screamingfrog import Crawl

old = Crawl.load("./crawl-2024-01.dbseospider")
new = Crawl.load("./crawl-2024-02.dbseospider")

diff = new.compare(old)

print(f"Added pages: {len(diff.added_pages)}")
print(f"Removed pages: {len(diff.removed_pages)}")
print(f"Status changes: {len(diff.status_changes)}")
print(f"Title changes: {len(diff.title_changes)}")
print(f"Redirect changes: {len(diff.redirect_changes)}")
print(f"Field changes: {len(diff.field_changes)}")
```

### Diff Signals

| Property | Type | Description |
|----------|------|-------------|
| `added_pages` | `list[str]` | URLs only in new crawl |
| `removed_pages` | `list[str]` | URLs only in old crawl |
| `status_changes` | `list[StatusChange]` | HTTP status changes |
| `title_changes` | `list[TitleChange]` | Title tag changes |
| `redirect_changes` | `list[RedirectChange]` | Redirect target changes |
| `field_changes` | `list[FieldChange]` | Other field changes |

### Default Field Groups

The diff tracks these fields by default:

- Canonical and Canonical Status
- Meta Description, Meta Keywords, Meta Refresh
- H1-1, H2-1, H3-1
- Word Count
- Indexability and Indexability Status
- Meta Robots, X-Robots-Tag
- Directives Summary

### Iterating Changes

```python
# Status changes
for change in diff.status_changes:
    print(f"{change.url}: {change.old_status} -> {change.new_status}")

# Title changes
for change in diff.title_changes:
    print(f"{change.url}: '{change.old_title}' -> '{change.new_title}'")

# Redirect changes
for change in diff.redirect_changes:
    print(f"{change.url}: {change.old_target} -> {change.new_target}")
    print(f"  Type: {change.old_type} -> {change.new_type}")

# Field changes
for change in diff.field_changes:
    print(f"{change.url} [{change.field}]: '{change.old_value}' -> '{change.new_value}'")
```

### Customizing Diff

```python
diff = new.compare(
    old,
    # Override title field candidates
    title_fields=("Title 1", "Title", "TITLE"),

    # Override redirect field candidates
    redirect_fields=("Redirect URL", "Redirect URI"),
    redirect_type_fields=("Redirect Type",),

    # Custom field groups to track
    field_groups={
        "Canonical": ("Canonical Link Element 1",),
        "H1-1": ("H1-1", "H1"),
        # ... add custom fields
    }
)
```

---

## Raw SQL Access

For advanced users who need direct database access.

### Raw Table Iteration

```python
# Iterate raw Derby/SQLite table
for row in crawl.raw("APP.URLS"):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"])

# Available tables (Derby)
# - APP.URLS: Main URL data
# - APP.LINKS: Link relationships
# - APP.UNIQUE_URLS: URL deduplication
# - APP.HTML_VALIDATION_DATA: HTML validation flags
# - APP.DUPLICATES_*: Duplicate detection tables
```

### SQL Passthrough

```python
# Execute raw SQL
for row in crawl.sql(
    "SELECT ENCODED_URL, RESPONSE_CODE FROM APP.URLS WHERE RESPONSE_CODE >= ?",
    [400]
):
    print(row)

# Join tables
for row in crawl.sql("""
    SELECT u.ENCODED_URL, u.RESPONSE_CODE, u.TITLE_1
    FROM APP.URLS u
    WHERE u.RESPONSE_CODE BETWEEN 300 AND 399
    ORDER BY u.RESPONSE_CODE
"""):
    print(row)
```

### Notes

- `raw()` and `sql()` only work with Derby and SQLite backends
- CSV/CLI backends raise `NotImplementedError`
- Column names vary by backend and SF version

---

## CLI Exports

Automate Screaming Frog exports from Python.

### Export Function

```python
from screamingfrog.cli.exports import export_crawl

export_dir = export_crawl(
    "./crawl.seospider",
    "./exports",
    export_tabs=["Internal:All", "Response Codes:All", "Page Titles:All"],
    bulk_exports=["Bulk Export: Sitemaps"],
    export_format="csv",
    headless=True,
    overwrite=True,
)
```

### Kitchen-Sink Profile

```python
from screamingfrog.config import get_export_profile

# Get bundled comprehensive export list
profile = get_export_profile("kitchen_sink")
print(f"Tabs: {len(profile.export_tabs)}")
print(f"Bulk exports: {len(profile.bulk_exports)}")

# Use in export
export_crawl(
    "./crawl.seospider",
    "./exports",
    export_profile="kitchen_sink",
)
```

### CLI Path Resolution

The CLI is found automatically:

1. Explicit `cli_path` parameter
2. `SCREAMINGFROG_CLI` environment variable
3. Windows: `C:\Program Files (x86)\Screaming Frog SEO Spider\`
4. macOS: `/Applications/Screaming Frog SEO Spider.app/`
5. Linux: `/usr/bin/screamingfrogseospider`

---

## Database Packaging

Work with `.dbseospider` files for portability.

### Pack/Unpack Functions

```python
from screamingfrog import (
    pack_dbseospider,
    unpack_dbseospider,
    pack_dbseospider_from_db_id,
    export_dbseospider_from_seospider,
)

# Pack a DB-mode crawl folder
dbseospider = pack_dbseospider(
    r"C:\Users\...\ProjectInstanceData\<project_id>",
    r"C:\output\my-crawl.dbseospider"
)

# Pack by crawl ID
dbseospider = pack_dbseospider_from_db_id(
    "7c356a1b-ea14-40f3-b504-36c3046432a2",
    r"C:\output\my-crawl.dbseospider"
)

# Unpack to directory
unpack_dbseospider(
    r"C:\my-crawl.dbseospider",
    r"C:\unpacked"
)

# Convert .seospider to .dbseospider
dbseospider = export_dbseospider_from_seospider(
    r"C:\my-crawl.seospider",
    r"C:\output\converted.dbseospider"
)
```

---

## Config Patches

Build configuration patches for custom crawl settings.

### ConfigPatches API

```python
from screamingfrog import ConfigPatches, CustomSearch, CustomJavaScript

patches = ConfigPatches()

# Set arbitrary config values
patches.set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")
patches.set("mCrawlConfig.mMaxUrls", 10000)

# Add custom search (regex pattern)
patches.add_custom_search(
    CustomSearch(
        name="PDF Links",
        query=r"\.pdf$",
        data_type="REGEX",
        mode="MATCHES"
    )
)

# Add custom JavaScript extraction
patches.add_custom_javascript(
    CustomJavaScript(
        name="Page Title",
        javascript="return document.title;",
        type="EXTRACTION"
    )
)

# Add XPath extraction
patches.add_extraction(
    name="H1 Text",
    selector="//h1",
    selector_type="XPATH",
    extract_mode="TEXT"
)

# Export as JSON
json_str = patches.to_json()
```

---

## Data Models

### InternalPage

```python
@dataclass(frozen=True)
class InternalPage:
    address: str                    # URL
    status_code: Optional[int]      # HTTP status
    id: Optional[int]               # Database ID
    data: dict[str, Any]            # Full row data
```

### Link

```python
@dataclass(frozen=True)
class Link:
    source: Optional[str]           # Source URL
    destination: Optional[str]      # Destination URL
    anchor_text: Optional[str]      # Anchor text
    data: dict[str, Any]            # Link metadata
```

### CrawlDiff

```python
@dataclass(frozen=True)
class CrawlDiff:
    added_pages: list[str]
    removed_pages: list[str]
    status_changes: list[StatusChange]
    title_changes: list[TitleChange]
    redirect_changes: list[RedirectChange]
    field_changes: list[FieldChange]
```

### Change Types

```python
@dataclass(frozen=True)
class StatusChange:
    url: str
    old_status: Optional[int]
    new_status: Optional[int]

@dataclass(frozen=True)
class TitleChange:
    url: str
    old_title: Optional[str]
    new_title: Optional[str]

@dataclass(frozen=True)
class RedirectChange:
    url: str
    old_target: Optional[str]
    new_target: Optional[str]
    old_type: Optional[str]
    new_type: Optional[str]

@dataclass(frozen=True)
class FieldChange:
    url: str
    field: str
    old_value: Optional[str]
    new_value: Optional[str]
```

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `SCREAMINGFROG_CLI` | Path to Screaming Frog CLI executable |
| `SCREAMINGFROG_PROJECT_DIR` | ProjectInstanceData root directory |
| `SCREAMINGFROG_MAPPING` | Custom Derby column mapping JSON |
| `SCREAMINGFROG_SPIDER_CONFIG` | Path to spider.config file |
| `DERBY_JAR` | Derby jar path(s), pathsep-separated |
| `JAVA_HOME` | Java runtime directory |

---

## Use Cases

### 1. Broken Links Report

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

print("=== Broken Links Report ===\n")

for page in crawl.internal.filter(status_code=404):
    print(f"404: {page.address}")
    for link in crawl.inlinks(page.address):
        print(f"  <- {link.source}")
        print(f"     Anchor: {link.anchor_text}")
```

### 2. Title and Meta Audit

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

print("=== Missing Titles ===")
for row in crawl.tab("page_titles").filter(gui="Missing"):
    print(row["Address"])

print("\n=== Missing Meta Descriptions ===")
for row in crawl.tab("meta_description").filter(gui="Missing"):
    print(row["Address"])

print("\n=== Duplicate Titles ===")
for row in crawl.tab("page_titles").filter(gui="Duplicate"):
    print(f"{row['Address']}: {row.get('Title 1')}")
```

### 3. Crawl Diff for Releases

```python
from screamingfrog import Crawl

before = Crawl.load("./pre-release.dbseospider")
after = Crawl.load("./post-release.dbseospider")

diff = after.compare(before)

print("=== Release Impact Report ===\n")

if diff.added_pages:
    print(f"New pages: {len(diff.added_pages)}")
    for url in diff.added_pages[:10]:
        print(f"  + {url}")

if diff.removed_pages:
    print(f"\nRemoved pages: {len(diff.removed_pages)}")
    for url in diff.removed_pages[:10]:
        print(f"  - {url}")

if diff.status_changes:
    print(f"\nStatus changes: {len(diff.status_changes)}")
    for change in diff.status_changes[:10]:
        print(f"  {change.url}: {change.old_status} -> {change.new_status}")
```

### 4. Indexability Review

```python
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

non_indexable = []
for page in crawl.internal:
    idx = page.data.get("Indexability", "")
    if "Non-Indexable" in str(idx):
        status = page.data.get("Indexability Status", "Unknown")
        non_indexable.append((page.address, status))

print(f"Non-indexable pages: {len(non_indexable)}\n")
for url, reason in non_indexable[:20]:
    print(f"  {url}")
    print(f"    Reason: {reason}")
```

### 5. Internal Link Analysis

```python
from screamingfrog import Crawl
from collections import Counter

crawl = Crawl.load("./crawl.dbseospider")

# Count inlinks per page
inlink_counts = Counter()
for page in crawl.internal:
    count = sum(1 for _ in crawl.inlinks(page.address))
    inlink_counts[page.address] = count

# Find orphan pages (0 inlinks)
orphans = [url for url, count in inlink_counts.items() if count == 0]
print(f"Orphan pages: {len(orphans)}")

# Find most linked pages
print("\nMost linked pages:")
for url, count in inlink_counts.most_common(10):
    print(f"  {count} links: {url}")
```

---

## Known Limitations

### Backend Limitations

| Backend | Limitation |
|---------|------------|
| CSV | No inlinks/outlinks, no raw SQL |
| SQLite | Limited tabs (internal, response codes, titles, meta) |
| Derby | Requires Java runtime |
| Hybrid | CSV fallback slower than pure Derby |

### Feature Gaps

- Some GUI filters are not fully implemented in Derby
- Some columns are mapped to NULL in Derby (use CSV fallback)
- `.seospider` files must be loaded via CLI (no direct deserialization)
- Pixel-width filters require CSV exports

### Dependencies

- **Derby backend**: Java runtime + Derby jars (bundled)
- **CLI automation**: Screaming Frog CLI installed
- **DB mode**: Screaming Frog license for DB storage mode

---

## Where to Look Next

- **Mapping gaps**: `schemas/mapping_nulls.md`
- **DB coverage**: `db_vs_schema_report.md`
- **Example scripts**: `examples/`
- **Feature breakdown**: `features.d/`
