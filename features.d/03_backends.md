# Backends

This document covers the backend implementations and their capabilities.

---

## Backend Overview

The library supports five backends, each with different capabilities:

| Backend | Source | Speed | Tabs | Filters | Links | SQL |
|---------|--------|-------|------|---------|-------|-----|
| CSVBackend | CSV exports | Slow | All CSV | File-based | No | No |
| DatabaseBackend | SQLite | Fast | Limited | Limited | No | Yes |
| DerbyBackend | .dbseospider | Fast | Mapped | SQL-based | Yes | Yes |
| CLIExportBackend | .seospider | Varies | Via CLI | Via CLI | No | No |
| HybridBackend | Derby+CSV | Fast | All | Fallback | Yes | Yes |

---

## CSV Backend

### Description

Reads CSV export folders created by Screaming Frog's export feature.

### Capabilities

| Feature | Supported |
|---------|-----------|
| Internal view | ✓ |
| Generic tabs | ✓ (any CSV file) |
| GUI filters | ✓ (file-based) |
| Inlinks/Outlinks | ✗ |
| Raw SQL | ✗ |
| Aggregations | ✗ |

### How It Works

- Discovers all `*.csv` files in the export directory
- Auto-detects `internal_all.csv`, `Internal All.csv`, or `internal.csv` for internal view
- GUI filters resolved by selecting matching CSV files (e.g., `page_titles_missing.csv`)
- Column filtering done via row iteration

### Usage

```python
from screamingfrog import Crawl

crawl = Crawl.from_exports("./exports")

# Or via Crawl.load
crawl = Crawl.load("./exports")  # Auto-detects CSV folder
```

### Tab Name Resolution

Tab names are normalized:
- `"Page Titles"` → `page_titles_all.csv`
- `"page_titles"` → `page_titles_all.csv`
- `"page_titles.csv"` → `page_titles.csv`
- `"page_titles_missing"` → `page_titles_missing.csv`

### Limitations

- No inlinks/outlinks support
- No raw SQL access
- Slow for large crawls (full file scan)
- GUI filter availability depends on exported files

---

## SQLite Backend

### Description

Reads SQLite database files created by Screaming Frog.

### Capabilities

| Feature | Supported |
|---------|-----------|
| Internal view | ✓ |
| Generic tabs | Limited set |
| GUI filters | Limited |
| Inlinks/Outlinks | ✗ |
| Raw SQL | ✓ |
| Aggregations | ✓ |

### Supported Tabs

The SQLite backend supports these high-value tabs:

| Tab | Description |
|-----|-------------|
| `internal_all.csv` | All internal URLs |
| `response_codes_internal_all.csv` | All internal response codes |
| `response_codes_internal_success_(2xx).csv` | Internal 2xx responses |
| `response_codes_internal_redirection_(3xx).csv` | Internal 3xx responses |
| `response_codes_internal_client_error_(4xx).csv` | Internal 4xx responses |
| `response_codes_internal_server_error_(5xx).csv` | Internal 5xx responses |
| `response_codes_internal_no_response.csv` | Internal no response |
| `page_titles_all.csv` | All page titles |
| `page_titles_missing.csv` | Missing titles |
| `meta_description_all.csv` | All meta descriptions |
| `meta_description_missing.csv` | Missing meta descriptions |

### Supported GUI Filters

- `Page Titles:Missing`
- `Meta Description:Missing`

### Usage

```python
from screamingfrog import Crawl

crawl = Crawl.from_database("./crawl.db")

# Or via Crawl.load
crawl = Crawl.load("./crawl.sqlite")
```

### Limitations

- Limited tab support (not full GUI parity)
- No inlinks/outlinks
- Column names may vary by SF version

---

## Derby Backend

### Description

Reads Apache Derby databases from `.dbseospider` files or DB-mode project folders.

### Capabilities

| Feature | Supported |
|---------|-----------|
| Internal view | ✓ |
| Generic tabs | ✓ (via mapping) |
| GUI filters | ✓ (SQL-based) |
| Inlinks/Outlinks | ✓ |
| Raw SQL | ✓ |
| Aggregations | ✓ |

### How It Works

- Uses `schemas/mapping.json` to map Derby columns to CSV column names
- Parses HTTP response header blobs for HTTP canonical/rel fields
- Link types labeled (Hyperlink, Canonical, Rel Prev/Next, Hreflang)
- Chain reports computed in Python

### Derby Tables

| Table | Description |
|-------|-------------|
| `APP.URLS` | Main URL data with all fields |
| `APP.LINKS` | Link relationships |
| `APP.UNIQUE_URLS` | URL deduplication |
| `APP.HTML_VALIDATION_DATA` | HTML validation flags |
| `APP.DUPLICATES_TITLE` | Duplicate title detection |
| `APP.DUPLICATES_META_DESCRIPTION` | Duplicate meta detection |
| `APP.MULTIMAP_*` | Various multimap tables |

### Usage

```python
from screamingfrog import Crawl

crawl = Crawl.from_derby("./crawl.dbseospider")

# With custom options
crawl = Crawl.from_derby(
    "./crawl.dbseospider",
    mapping_path="/custom/mapping.json",
    derby_jar="/path/to/derby.jar",
)
```

### Requirements

- Java runtime (JRE 8+)
- Derby jars (bundled with package)

### Limitations

- Some columns mapped to NULL (use CSV fallback)
- Some GUI filters not fully implemented
- Requires Java runtime

---

## CLI Export Backend

### Description

Uses the Screaming Frog CLI to export data, wrapping CSVBackend.

### Capabilities

| Feature | Supported |
|---------|-----------|
| Internal view | ✓ |
| Generic tabs | ✓ (exported tabs) |
| GUI filters | ✓ (via export) |
| Inlinks/Outlinks | ✗ |
| Raw SQL | ✗ |
| Aggregations | ✗ |

### How It Works

1. Calls SF CLI with `--load-crawl` and export flags
2. Creates temporary export directory if not specified
3. Wraps CSVBackend on exported files

### Usage

```python
from screamingfrog import Crawl

# Load .seospider with CSV export
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_tabs=["Internal:All", "Page Titles:All"],
)
```

### Limitations

- Requires SF CLI installed
- Export time depends on crawl size
- No inlinks/outlinks
- No raw SQL

---

## Hybrid Backend

### Description

Combines Derby backend (primary) with CSV fallback for missing features.

### Capabilities

| Feature | Supported |
|---------|-----------|
| Internal view | ✓ |
| Generic tabs | ✓ (all) |
| GUI filters | ✓ (with fallback) |
| Inlinks/Outlinks | ✓ |
| Raw SQL | ✓ |
| Aggregations | ✓ |

### How It Works

1. Attempts Derby for all operations
2. Detects missing mappings or unsupported filters
3. Automatically exports missing tabs via CLI
4. Caches CSV exports for reuse

### Fallback Triggers

Fallback to CSV occurs when:

- A GUI filter has no SQL implementation
- A mapped column resolves to NULL
- A tab has no Derby mapping

### Usage

```python
from screamingfrog import Crawl

# Hybrid is the default for .dbseospider
crawl = Crawl.load("./crawl.dbseospider", csv_fallback=True)

# Configure fallback
crawl = Crawl.load(
    "./crawl.dbseospider",
    csv_fallback=True,
    csv_fallback_cache_dir="./cache",
    csv_fallback_profile="kitchen_sink",
    csv_fallback_warn=True,
)

# Disable fallback
crawl = Crawl.load("./crawl.dbseospider", csv_fallback=False)
```

### Performance Notes

- Derby operations are fast
- CSV fallback adds export overhead on first use
- Cached exports reused on subsequent calls

---

## Backend Comparison Matrix

### Feature Support

| Feature | CSV | SQLite | Derby | CLI | Hybrid |
|---------|-----|--------|-------|-----|--------|
| `crawl.internal` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `crawl.internal.filter()` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `crawl.internal.count()` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `crawl.tab(name)` | ✓ | Limited | ✓ | ✓ | ✓ |
| `crawl.tab().filter(gui=)` | ✓ | Limited | ✓ | ✓ | ✓ |
| `crawl.tabs` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `crawl.inlinks(url)` | ✗ | ✗ | ✓ | ✗ | ✓ |
| `crawl.outlinks(url)` | ✗ | ✗ | ✓ | ✗ | ✓ |
| `crawl.raw(table)` | ✗ | ✓ | ✓ | ✗ | ✓ |
| `crawl.sql(query)` | ✗ | ✓ | ✓ | ✗ | ✓ |
| `crawl.compare()` | ✓ | ✓ | ✓ | ✓ | ✓ |

### Performance Characteristics

| Backend | Load Time | Query Speed | Memory Usage |
|---------|-----------|-------------|--------------|
| CSV | Fast | Slow (scan) | Low |
| SQLite | Fast | Fast | Medium |
| Derby | Medium | Fast | Medium |
| CLI | Slow (export) | Slow | Low |
| Hybrid | Medium | Fast | Medium |

### Recommended Use Cases

| Use Case | Recommended Backend |
|----------|---------------------|
| Quick analysis | Derby |
| Full GUI parity | Hybrid or CSV |
| Link graph analysis | Derby or Hybrid |
| Raw SQL queries | Derby or SQLite |
| Automation pipelines | Hybrid |
| Large crawls | Derby |

---

## Backend Selection Guide

### Choose Derby When:
- You need inlinks/outlinks
- You need fast queries
- You need raw SQL access
- You have Java installed

### Choose Hybrid When:
- You need all Derby features
- Plus full GUI filter support
- And are OK with occasional CSV exports

### Choose CSV When:
- You need exact GUI parity
- You don't need links
- You already have exports

### Choose SQLite When:
- You have SQLite files
- You need basic analysis
- You need raw SQL

### Choose CLI When:
- You only have .seospider files
- You need specific exports
- You don't have Java

---

## Internal Backend Interface

All backends implement the `CrawlBackend` abstract class:

```python
class CrawlBackend(ABC):
    @abstractmethod
    def get_internal(self, filters=None) -> Iterator[InternalPage]:
        """Iterate internal pages with optional filters."""

    @abstractmethod
    def get_inlinks(self, url: str) -> Iterator[Link]:
        """Get inlinks to a URL (may raise NotImplementedError)."""

    @abstractmethod
    def get_outlinks(self, url: str) -> Iterator[Link]:
        """Get outlinks from a URL (may raise NotImplementedError)."""

    @abstractmethod
    def count(self, table: str, filters=None) -> int:
        """Count rows in a table."""

    @abstractmethod
    def aggregate(self, table: str, column: str, func: str) -> Any:
        """Aggregate function on a column."""

    @abstractmethod
    def list_tabs(self) -> list[str]:
        """List available tab names."""

    @abstractmethod
    def get_tab(self, name: str, filters=None) -> Iterator[dict]:
        """Iterate tab rows with optional filters."""

    def raw(self, table: str) -> Iterator[dict]:
        """Raw table iteration (DB only)."""
        raise NotImplementedError

    def sql(self, query: str, params=None) -> Iterator[dict]:
        """SQL passthrough (DB only)."""
        raise NotImplementedError
```
