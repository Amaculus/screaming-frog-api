# Core API and Data Models

This document covers the primary entry points, classes, and data models of the library.

---

## Primary Entry Point: `Crawl`

The `Crawl` class is the main interface for accessing crawl data.

```python
from screamingfrog import Crawl
```

---

## Crawl Construction

### Unified Loader (Recommended)

```python
crawl = Crawl.load(path, **options)
```

Auto-detects the source type based on path characteristics.

### Alternative Constructors

| Constructor | Description | Backend |
|-------------|-------------|---------|
| `Crawl.from_exports(export_dir)` | CSV export folder | CSVBackend |
| `Crawl.from_database(db_path)` | SQLite database | DatabaseBackend |
| `Crawl.from_derby(db_path, ...)` | Derby `.dbseospider` or project dir | DerbyBackend/HybridBackend |
| `Crawl.from_seospider(crawl_path, ...)` | Load `.seospider` via CLI | DerbyBackend/CSVBackend |
| `Crawl.from_db_id(crawl_id, ...)` | DB-mode crawl by ID | DerbyBackend |

### Constructor Examples

```python
from screamingfrog import Crawl

# CSV exports folder
crawl = Crawl.from_exports("./exports")

# SQLite database
crawl = Crawl.from_database("./crawl.db")

# Derby .dbseospider file
crawl = Crawl.from_derby("./crawl.dbseospider")

# Derby with custom options
crawl = Crawl.from_derby(
    "./crawl.dbseospider",
    mapping_path="/custom/mapping.json",
    derby_jar="/path/to/derby.jar",
    csv_fallback=True,
    csv_fallback_profile="kitchen_sink",
)

# .seospider via CLI (Derby mode)
crawl = Crawl.from_seospider("./crawl.seospider", backend="derby")

# .seospider via CLI (CSV mode)
crawl = Crawl.from_seospider(
    "./crawl.seospider",
    backend="csv",
    export_dir="./exports",
    export_profile="kitchen_sink",
)

# DB crawl ID
crawl = Crawl.from_db_id("138edb21-61d0-41cd-9e9b-725b592a471c")
```

---

## Core Views and Iteration

### `crawl.internal` - Typed Internal View

Returns an `InternalView` that yields `InternalPage` objects.

```python
# Iterate all internal pages
for page in crawl.internal:
    print(page.address, page.status_code)

# Chain filter calls
for page in crawl.internal.filter(status_code=404):
    print(page.address)

# Count with optional filters
total = crawl.internal.count()
errors = crawl.internal.filter(status_code=404).count()
```

### `crawl.tab(name)` - Generic Tab Access

Returns a `TabView` that yields dict rows.

```python
# Access any export tab
for row in crawl.tab("response_codes_all"):
    print(row["Address"], row["Status Code"])

# Apply column filters
for row in crawl.tab("internal_all").filter(status_code=404):
    print(row["Address"])

# Apply GUI filters
for row in crawl.tab("page_titles").filter(gui="Missing"):
    print(row["Address"])
```

### `crawl.tabs` - List Available Tabs

Returns a list of available tab names.

```python
tabs = crawl.tabs
print(tabs)  # ['internal_all.csv', 'response_codes_all.csv', ...]
```

---

## Link Traversal (Derby Only)

### `crawl.inlinks(url)` - Inbound Links

```python
for link in crawl.inlinks("https://example.com/page"):
    print(f"{link.source} -> {link.destination}")
    print(f"  Anchor: {link.anchor_text}")
    print(f"  Type: {link.data.get('Link Type')}")
```

### `crawl.outlinks(url)` - Outbound Links

```python
for link in crawl.outlinks("https://example.com/page"):
    print(f"{link.source} -> {link.destination}")
```

### Link Object Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `source` | `Optional[str]` | Source URL |
| `destination` | `Optional[str]` | Destination URL |
| `anchor_text` | `Optional[str]` | Link anchor text |
| `data` | `dict[str, Any]` | Full link metadata |

---

## Crawl Comparison

### `crawl.compare(other, ...)` - Diff Two Crawls

```python
old = Crawl.load("./old.dbseospider")
new = Crawl.load("./new.dbseospider")

diff = new.compare(old)

# Access change lists
print(f"Added: {len(diff.added_pages)}")
print(f"Removed: {len(diff.removed_pages)}")
print(f"Status changes: {len(diff.status_changes)}")
print(f"Title changes: {len(diff.title_changes)}")
print(f"Redirect changes: {len(diff.redirect_changes)}")
print(f"Field changes: {len(diff.field_changes)}")
```

### Compare Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `title_fields` | `Sequence[str]` | `("Title 1", "Title")` | Title column candidates |
| `redirect_fields` | `Sequence[str]` | `("Redirect URL", ...)` | Redirect target candidates |
| `redirect_type_fields` | `Sequence[str]` | `("Redirect Type",)` | Redirect type candidates |
| `field_groups` | `dict[str, Sequence[str]]` | Default groups | Custom field groups to track |

---

## Tab Metadata Helpers

### `crawl.tab_filters(name)` - List GUI Filters

```python
filters = crawl.tab_filters("Page Titles")
print(filters)  # ["All", "Missing", "Duplicate", ...]
```

### `crawl.tab_filter_defs(name)` - Full Filter Definitions

```python
filter_defs = crawl.tab_filter_defs("Page Titles")
for filt in filter_defs:
    print(f"{filt.name}: {filt.description}")
    print(f"  SQL: {filt.sql_where}")
```

### `crawl.tab_columns(name)` - Column Names

```python
columns = crawl.tab_columns("page_titles")
print(columns)  # ["Address", "Title 1", "Title 1 Length", ...]
```

### `crawl.describe_tab(name)` - Combined Metadata

```python
metadata = crawl.describe_tab("page_titles")
print(metadata)
# {
#     "tab": "page_titles",
#     "columns": ["Address", "Title 1", ...],
#     "filters": ["All", "Missing", ...]
# }
```

---

## Raw Database Access (Escape Hatches)

### `crawl.raw(table)` - Raw Table Rows

```python
for row in crawl.raw("APP.URLS"):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"])
```

### `crawl.sql(query, params)` - SQL Passthrough

```python
for row in crawl.sql(
    "SELECT ENCODED_URL, RESPONSE_CODE FROM APP.URLS WHERE RESPONSE_CODE >= ?",
    [400]
):
    print(row)
```

---

## Data Models

### `InternalPage`

Represents a single row from the Internal tab/table.

```python
from screamingfrog.models import InternalPage

@dataclass(frozen=True)
class InternalPage:
    address: str                    # URL of the page
    status_code: Optional[int]      # HTTP status code
    id: Optional[int]               # Database row ID (DB backends)
    data: dict[str, Any]            # Full row data as dictionary
```

**Class Methods:**

| Method | Description |
|--------|-------------|
| `from_csv_row(row)` | Create from CSV dict row |
| `from_db_row(columns, values)` | Create from DB result tuple |

**Usage:**

```python
for page in crawl.internal:
    # Primary attributes
    print(page.address)       # "https://example.com/page"
    print(page.status_code)   # 200
    print(page.id)            # 42 (or None for CSV)

    # Access any column via data dict
    print(page.data.get("Title 1"))
    print(page.data.get("Word Count"))
    print(page.data.get("Indexability"))
```

### `Link`

Represents a link relationship between two URLs.

```python
from screamingfrog.models import Link

@dataclass(frozen=True)
class Link:
    source: Optional[str]           # Source URL
    destination: Optional[str]      # Destination URL
    anchor_text: Optional[str]      # Link anchor text
    data: dict[str, Any]            # Additional link metadata
```

**Data Dict Fields (Derby):**

| Field | Description |
|-------|-------------|
| `Rel` | Link rel attribute value |
| `NoFollow` | NoFollow flag |
| `Alt Text` | Image alt text |
| `Link Path` | DOM path to element |
| `Link Position` | Position on page |
| `Status Code` | Destination status code |
| `Content Type` | Destination content type |
| `Link Type` | Type label (Hyperlink, Canonical, etc.) |

### `CrawlDiff`

Result of comparing two crawls.

```python
from screamingfrog.models import CrawlDiff

@dataclass(frozen=True)
class CrawlDiff:
    added_pages: list[str]                    # URLs only in new crawl
    removed_pages: list[str]                  # URLs only in old crawl
    status_changes: list[StatusChange]        # HTTP status changes
    title_changes: list[TitleChange]          # Title tag changes
    redirect_changes: list[RedirectChange]    # Redirect target changes
    field_changes: list[FieldChange]          # Other field changes
```

### `StatusChange`

```python
@dataclass(frozen=True)
class StatusChange:
    url: str                        # Page URL
    old_status: Optional[int]       # Previous status code
    new_status: Optional[int]       # Current status code
```

### `TitleChange`

```python
@dataclass(frozen=True)
class TitleChange:
    url: str                        # Page URL
    old_title: Optional[str]        # Previous title
    new_title: Optional[str]        # Current title
```

### `RedirectChange`

```python
@dataclass(frozen=True)
class RedirectChange:
    url: str                        # Page URL
    old_target: Optional[str]       # Previous redirect target
    new_target: Optional[str]       # Current redirect target
    old_type: Optional[str]         # Previous redirect type
    new_type: Optional[str]         # Current redirect type
```

### `FieldChange`

```python
@dataclass(frozen=True)
class FieldChange:
    url: str                        # Page URL
    field: str                      # Field name (e.g., "Canonical")
    old_value: Optional[str]        # Previous value
    new_value: Optional[str]        # Current value
```

---

## View Classes

### `InternalView`

Wrapper for internal page iteration with filtering.

```python
@dataclass(frozen=True)
class InternalView:
    backend: CrawlBackend
    filters: dict[str, Any] | None = None

    def filter(self, **kwargs) -> "InternalView":
        """Return new view with additional filters."""
        ...

    def __iter__(self) -> Iterator[InternalPage]:
        """Iterate filtered internal pages."""
        ...

    def count(self) -> int:
        """Count matching pages."""
        ...
```

### `TabView`

Wrapper for generic tab iteration with filtering.

```python
@dataclass(frozen=True)
class TabView:
    backend: CrawlBackend
    name: str
    filters: dict[str, Any] | None = None

    def filter(self, **kwargs) -> "TabView":
        """Return new view with additional filters.

        Supports:
        - Column filters: filter(status_code=404)
        - GUI filters: filter(gui="Missing")
        - Multiple GUI filters: filter(gui_filters=["Missing", "Duplicate"])
        """
        ...

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Iterate filtered tab rows."""
        ...

    def count(self) -> int:
        """Count matching rows."""
        ...
```

---

## Filter Definitions

### `FilterDef`

Definition of a GUI-style filter.

```python
from screamingfrog.filters.registry import FilterDef

@dataclass(frozen=True)
class FilterDef:
    name: str                       # Filter name (e.g., "Missing")
    tab: str                        # Tab name (e.g., "Page Titles")
    description: str                # Human-readable description
    sql_where: Optional[str]        # SQL WHERE clause for Derby
    join_table: Optional[str]       # JOIN table if needed
    join_on: Optional[str]          # JOIN condition
    join_type: str                  # "LEFT" or "INNER"
    columns: list[str]              # Relevant column names
```

### Filter Registry Functions

```python
from screamingfrog.filters.registry import (
    list_tabs,        # List all tabs with filters
    list_filters,     # List filters for a tab
    get_filter,       # Get specific filter definition
    all_filters,      # Iterate all filter definitions
)

# List all tabs
tabs = list_tabs()

# List filters for a tab
filters = list_filters("Page Titles")

# Get specific filter
filt = get_filter("Page Titles", "Missing")
print(filt.sql_where)  # "TITLE_1 IS NULL OR TRIM(TITLE_1) = ''"
```

---

## Utility Exports

### Parse Internal Row

```python
from screamingfrog.exports import parse_internal_row

# Parse a CSV row dict into InternalPage
row = {"Address": "https://example.com", "Status Code": "200", ...}
page = parse_internal_row(row)
print(page.address, page.status_code)
```

---

## Type Hints

The library uses standard Python type hints:

```python
from typing import Any, Iterator, Optional, Sequence

# Common return types
Iterator[InternalPage]      # Internal page iteration
Iterator[Link]              # Link iteration
Iterator[dict[str, Any]]    # Generic tab/raw iteration
list[str]                   # Tab names, filter names, URLs
list[FilterDef]             # Filter definitions
Optional[int]               # Nullable integers
dict[str, Any]              # Row data, metadata
```
