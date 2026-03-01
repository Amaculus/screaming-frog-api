# Sources and Loaders

This document covers all data sources and loader options supported by the library.

---

## `Crawl.load()` Auto-Detection

The unified `Crawl.load(path)` method auto-detects the source type based on path characteristics.

### Detection Rules

| Path Characteristic | Source Type | Backend |
|---------------------|-------------|---------|
| Directory containing `*.csv` files | CSV exports | CSVBackend |
| Directory containing `service.properties` | Derby project | DerbyBackend |
| File with `.dbseospider` extension | Derby archive | DerbyBackend |
| File with `.sqlite` or `.db` extension | SQLite database | DatabaseBackend |
| File with `.seospider` extension | SF crawl file | CLI → Derby/CSV |
| Non-existent path matching UUID pattern | DB crawl ID | DerbyBackend |

### Example

```python
from screamingfrog import Crawl

# Auto-detect handles all these:
crawl = Crawl.load("./exports")                    # CSV
crawl = Crawl.load("./crawl.db")                   # SQLite
crawl = Crawl.load("./crawl.dbseospider")          # Derby
crawl = Crawl.load("./crawl.seospider")            # CLI load
crawl = Crawl.load("138edb21-61d0-41cd-9e9b-725b592a471c")  # DB ID
```

---

## Supported Source Types

You can force a specific loader using `source_type`:

| Value | Description |
|-------|-------------|
| `"auto"` | Auto-detect (default) |
| `"exports"` or `"csv"` | CSV exports backend |
| `"sqlite"` or `"db"` | SQLite backend |
| `"derby"` or `"dbseospider"` | Derby backend |
| `"seospider"` | CLI load for `.seospider` files |
| `"db_id"` or `"database_id"` or `"dbid"` | DB-mode crawl by ID |

```python
# Force specific loader
crawl = Crawl.load("./data", source_type="csv")
crawl = Crawl.load("./data", source_type="derby")
```

---

## Complete Parameter Reference

### `Crawl.load()` Parameters

```python
Crawl.load(
    path: str,
    *,
    # Source type override
    source_type: str = "auto",

    # .seospider loader options
    seospider_backend: str = "derby",           # "derby" or "csv"
    materialize_dbseospider: bool = True,       # Create .dbseospider cache
    dbseospider_path: str | None = None,        # Custom cache path
    dbseospider_overwrite: bool = True,         # Overwrite existing cache
    ensure_db_mode: bool = True,                # Force DB storage mode
    spider_config_path: str | None = None,      # Custom spider.config path
    project_root: str | None = None,            # ProjectInstanceData root

    # CLI export options
    cli_path: str | None = None,                # CLI executable path
    export_dir: str | None = None,              # Export destination
    export_tabs: Sequence[str] | None = None,   # Tab:Filter labels
    bulk_exports: Sequence[str] | None = None,  # Bulk export labels
    save_reports: Sequence[str] | None = None,  # Report labels
    export_format: str = "csv",                 # Export format
    export_profile: str | None = None,          # Named profile
    headless: bool = True,                      # Run CLI headless
    overwrite: bool = True,                     # Overwrite exports
    force_export: bool = False,                 # Force re-export

    # Derby backend options
    mapping_path: str | None = None,            # Custom mapping JSON
    derby_jar: str | None = None,               # Derby jar path(s)

    # Hybrid fallback options
    csv_fallback: bool = True,                  # Enable CSV fallback
    csv_fallback_cache_dir: str | None = None,  # Fallback cache location
    csv_fallback_profile: str = "kitchen_sink", # Fallback export profile
    csv_fallback_warn: bool = True,             # Warn on fallback

    # DB ID loader options
    db_id_backend: str = "derby",               # "derby" or "csv"
) -> Crawl
```

---

## .seospider Loader Options

When loading `.seospider` files, the library uses the Screaming Frog CLI.

### Backend Selection

```python
# Derby mode (default) - converts to DB, uses Derby backend
crawl = Crawl.load("./crawl.seospider", seospider_backend="derby")

# CSV mode - exports to CSV, uses CSV backend
crawl = Crawl.load("./crawl.seospider", seospider_backend="csv")
```

### .dbseospider Caching

For Derby mode, the library creates a `.dbseospider` cache file by default:

```python
# Create cache (default)
crawl = Crawl.load(
    "./crawl.seospider",
    materialize_dbseospider=True,      # Creates crawl.dbseospider
    dbseospider_overwrite=True,        # Overwrite existing
)

# Custom cache path
crawl = Crawl.load(
    "./crawl.seospider",
    dbseospider_path="./cache/my-crawl.dbseospider",
)

# Reuse existing cache
crawl = Crawl.load(
    "./crawl.seospider",
    dbseospider_overwrite=False,       # Use existing if present
)

# No cache (uses ProjectInstanceData directly)
crawl = Crawl.load(
    "./crawl.seospider",
    materialize_dbseospider=False,
)
```

### Storage Mode

The library can temporarily force DB storage mode:

```python
# Force DB mode (default)
crawl = Crawl.load(
    "./crawl.seospider",
    ensure_db_mode=True,               # Sets storage.mode=DB in spider.config
)

# Skip DB mode enforcement
crawl = Crawl.load(
    "./crawl.seospider",
    ensure_db_mode=False,              # Use current storage mode
)
```

---

## Derby Loader Options

### Custom Mapping

```python
crawl = Crawl.load(
    "./crawl.dbseospider",
    mapping_path="/path/to/custom/mapping.json",  # Override bundled mapping
)
```

### Custom Derby Jars

```python
crawl = Crawl.load(
    "./crawl.dbseospider",
    derby_jar="/path/to/derby.jar:/path/to/derbyshared.jar",
)
```

Or via environment variable:

```bash
export DERBY_JAR="/path/to/derby.jar:/path/to/derbyshared.jar"
```

---

## Hybrid Fallback Options

The Hybrid backend uses Derby as primary, falling back to CSV exports for missing mappings.

### Enable/Disable Fallback

```python
# Enable fallback (default)
crawl = Crawl.load("./crawl.dbseospider", csv_fallback=True)

# Disable fallback (pure Derby)
crawl = Crawl.load("./crawl.dbseospider", csv_fallback=False)
```

### Fallback Cache

```python
crawl = Crawl.load(
    "./crawl.dbseospider",
    csv_fallback=True,
    csv_fallback_cache_dir="./exports_cache",   # Where to store CSV exports
    csv_fallback_profile="kitchen_sink",        # What to export
    csv_fallback_warn=True,                     # Warn on fallback
)
```

### When Fallback Triggers

Fallback to CSV occurs when:

1. A GUI filter is not implemented in Derby
2. A mapped column resolves to NULL
3. A tab has no Derby mapping

---

## CLI Export Options

### Export Tabs

```python
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_tabs=[
        "Internal:All",
        "Response Codes:All",
        "Page Titles:All",
        "Page Titles:Missing",
    ],
)
```

### Bulk Exports

```python
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    bulk_exports=[
        "Bulk Export: Sitemaps",
        "Bulk Export: All Links",
    ],
)
```

### Export Profile

```python
# Use bundled kitchen-sink profile
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_profile="kitchen_sink",      # Exports all tabs + bulk exports
)
```

### Export Directory

```python
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_dir="./my_exports",          # Custom export destination
)
```

### CLI Path

```python
crawl = Crawl.load(
    "./crawl.seospider",
    cli_path=r"C:\Custom\Path\ScreamingFrogSEOSpiderCli.exe",
)
```

Or via environment variable:

```bash
export SCREAMINGFROG_CLI="C:\Program Files (x86)\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe"
```

---

## DB ID Loader Options

For DB-mode crawls identified by their database ID.

```python
# Load by DB ID (Derby backend)
crawl = Crawl.load(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    source_type="db_id",
)

# Load by DB ID (CSV backend)
crawl = Crawl.load(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    source_type="db_id",
    db_id_backend="csv",
)

# Custom ProjectInstanceData root
crawl = Crawl.load(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    source_type="db_id",
    project_root=r"C:\Custom\ProjectInstanceData",
)
```

---

## Alternative Constructors

### `Crawl.from_exports(export_dir)`

Direct CSV backend loader.

```python
crawl = Crawl.from_exports("./exports")
```

### `Crawl.from_database(db_path)`

Direct SQLite backend loader.

```python
crawl = Crawl.from_database("./crawl.db")
crawl = Crawl.from_database("./crawl.sqlite")
```

### `Crawl.from_derby(db_path, ...)`

Direct Derby backend loader with full options.

```python
crawl = Crawl.from_derby(
    "./crawl.dbseospider",
    mapping_path=None,                  # Custom mapping
    derby_jar=None,                     # Derby jar path
    csv_fallback=True,                  # Enable Hybrid
    csv_fallback_cache_dir=None,        # Fallback cache
    csv_fallback_profile="kitchen_sink",
    csv_fallback_warn=True,
    cli_path=None,                      # CLI for fallback
    export_format="csv",
    headless=True,
    overwrite=False,
)
```

### `Crawl.from_seospider(crawl_path, ...)`

Direct .seospider loader with full options.

```python
crawl = Crawl.from_seospider(
    "./crawl.seospider",
    export_dir=None,
    backend="derby",                    # "derby" or "csv"
    project_root=None,
    dbseospider_path=None,
    materialize_dbseospider=True,
    dbseospider_overwrite=True,
    ensure_db_mode=True,
    spider_config_path=None,
    cli_path=None,
    export_tabs=None,
    bulk_exports=None,
    save_reports=None,
    export_format="csv",
    headless=True,
    overwrite=True,
    force_export=False,
    export_profile=None,
    mapping_path=None,
    derby_jar=None,
    csv_fallback=True,
    csv_fallback_cache_dir=None,
    csv_fallback_profile="kitchen_sink",
    csv_fallback_warn=True,
)
```

### `Crawl.from_db_id(crawl_id, ...)`

Direct DB ID loader with full options.

```python
crawl = Crawl.from_db_id(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    export_dir=None,
    backend="derby",                    # "derby" or "csv"
    project_root=None,
    cli_path=None,
    export_tabs=None,
    bulk_exports=None,
    save_reports=None,
    export_format="csv",
    headless=True,
    overwrite=True,
    force_export=False,
    export_profile=None,
    mapping_path=None,
    derby_jar=None,
    csv_fallback=True,
    csv_fallback_cache_dir=None,
    csv_fallback_profile="kitchen_sink",
    csv_fallback_warn=True,
)
```

---

## Environment Variables

| Variable | Description | Default Search |
|----------|-------------|----------------|
| `SCREAMINGFROG_CLI` | CLI executable path | Standard install locations |
| `SCREAMINGFROG_PROJECT_DIR` | ProjectInstanceData root | OS-specific default |
| `SCREAMINGFROG_MAPPING` | Derby mapping JSON | `schemas/mapping.json` |
| `SCREAMINGFROG_SPIDER_CONFIG` | spider.config path | Auto-detect |
| `DERBY_JAR` | Derby jar paths (pathsep) | Bundled jars |
| `JAVA_HOME` | Java runtime | System PATH |

### CLI Path Resolution Order

1. Explicit `cli_path` parameter
2. `SCREAMINGFROG_CLI` environment variable
3. Windows: `C:\Program Files (x86)\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe`
4. Windows: `C:\Program Files\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe`
5. macOS: `/Applications/Screaming Frog SEO Spider.app/Contents/MacOS/ScreamingFrogSEOSpider`
6. Linux: `/usr/bin/screamingfrogseospider`
7. Linux: `/usr/local/bin/screamingfrogseospider`
8. System PATH

### ProjectInstanceData Resolution Order

1. Explicit `project_root` parameter
2. `SCREAMINGFROG_PROJECT_DIR` environment variable
3. Windows: `%APPDATA%\.ScreamingFrogSEOSpider\ProjectInstanceData`
4. macOS/Linux: `~/.ScreamingFrogSEOSpider/ProjectInstanceData`

---

## Common Patterns

### Quick Analysis (Derby)

```python
# Fast loading with Derby
crawl = Crawl.load("./crawl.dbseospider")
```

### Full Feature Access (Hybrid)

```python
# Derby with CSV fallback for missing features
crawl = Crawl.load(
    "./crawl.dbseospider",
    csv_fallback=True,
    csv_fallback_profile="kitchen_sink",
)
```

### Convert .seospider to .dbseospider

```python
# One-time conversion
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",
    materialize_dbseospider=True,
)
# Now crawl.dbseospider exists for future use
```

### Pure CSV Analysis

```python
# Force CSV for exact GUI parity
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_profile="kitchen_sink",
)
```

### Headless Automation

```python
# Fully automated, no GUI
crawl = Crawl.load(
    "./crawl.seospider",
    headless=True,
    export_profile="kitchen_sink",
)
```
