# CLI Exports

This document covers CLI automation, export profiles, and headless crawling.

---

## Overview

The library can drive the Screaming Frog CLI to:

- Export CSV tabs from existing crawl files
- Export bulk reports (sitemaps, all links, etc.)
- Convert `.seospider` files to Derby databases
- Run headless crawls

---

## CLI Path Resolution

The library locates the Screaming Frog CLI automatically:

### Resolution Order

1. Explicit `cli_path` parameter
2. `SCREAMINGFROG_CLI` environment variable
3. Platform-specific default locations:

**Windows:**
```
C:\Program Files (x86)\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe
C:\Program Files\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe
```

**macOS:**
```
/Applications/Screaming Frog SEO Spider.app/Contents/MacOS/ScreamingFrogSEOSpider
```

**Linux:**
```
/usr/bin/screamingfrogseospider
/usr/local/bin/screamingfrogseospider
```

4. System PATH

### Manual CLI Path

```python
# Via parameter
crawl = Crawl.load(
    "./crawl.seospider",
    cli_path=r"C:\Custom\Path\ScreamingFrogSEOSpiderCli.exe",
)

# Via environment variable
import os
os.environ["SCREAMINGFROG_CLI"] = r"C:\Custom\Path\ScreamingFrogSEOSpiderCli.exe"
crawl = Crawl.load("./crawl.seospider")
```

---

## Export Functions

### `export_crawl()` - Export Tabs from Crawl Files

Export CSV tabs from `.seospider`, `.dbseospider`, or DB-mode directories:

```python
from screamingfrog.cli import export_crawl

# Export specific tabs
export_crawl(
    load_target="./crawl.seospider",
    export_dir="./exports",
    export_tabs=[
        "Internal:All",
        "Response Codes:All",
        "Page Titles:All",
        "Page Titles:Missing",
    ],
)

# Export bulk reports
export_crawl(
    load_target="./crawl.seospider",
    export_dir="./exports",
    bulk_exports=[
        "Bulk Export: Sitemaps",
        "Bulk Export: All Links",
    ],
)

# Export both tabs and bulk reports
export_crawl(
    load_target="./crawl.seospider",
    export_dir="./exports",
    export_tabs=["Internal:All"],
    bulk_exports=["Bulk Export: All Links"],
)
```

### Function Parameters

```python
export_crawl(
    load_target: str,                  # Path to crawl file or directory
    export_dir: str,                   # Export destination directory
    export_tabs: list[str] = None,     # Tab:Filter labels to export
    bulk_exports: list[str] = None,    # Bulk export labels
    save_reports: list[str] = None,    # Report labels to save
    export_format: str = "csv",        # Export format (csv, xlsx)
    cli_path: str = None,              # Custom CLI path
    headless: bool = True,             # Run without GUI
    overwrite: bool = True,            # Overwrite existing files
)
```

---

## Tab:Filter Labels

Export tabs are specified as `Tab:Filter` labels matching the Screaming Frog GUI:

### Common Tab:Filter Labels

| Label | Description |
|-------|-------------|
| `Internal:All` | All internal URLs |
| `Internal:HTML` | Internal HTML pages |
| `Response Codes:All` | All response codes |
| `Response Codes:Client Error (4xx)` | 4xx errors |
| `Response Codes:Server Error (5xx)` | 5xx errors |
| `Page Titles:All` | All page titles |
| `Page Titles:Missing` | Missing titles |
| `Page Titles:Duplicate` | Duplicate titles |
| `Meta Description:All` | All meta descriptions |
| `Meta Description:Missing` | Missing descriptions |
| `H1:All` | All H1 headings |
| `H1:Missing` | Missing H1 |
| `Canonicals:All` | All canonicals |
| `Canonicals:Missing` | Missing canonicals |
| `Directives:All` | All directives |
| `Directives:Noindex` | Noindex pages |
| `Images:All` | All images |
| `Images:Missing Alt Text` | Missing alt text |
| `Hreflang:All` | All hreflang |
| `Structured Data:All` | All structured data |

### Bulk Export Labels

| Label | Description |
|-------|-------------|
| `Bulk Export: Sitemaps` | All sitemap data |
| `Bulk Export: All Links` | Complete link export |
| `Bulk Export: All Outlinks` | All outbound links |
| `Bulk Export: All Inlinks` | All inbound links |
| `Bulk Export: Response Times` | Response time data |
| `Bulk Export: Validation` | Validation data |

---

## Export Profiles

Export profiles define pre-configured sets of tabs and bulk exports.

### Built-in Profiles

#### kitchen_sink

Exports all tabs and bulk exports for maximum coverage:

```python
# Use kitchen_sink profile
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_profile="kitchen_sink",
)
```

The kitchen_sink profile includes:
- All tabs from `exports_kitchen_sink_tabs.txt`
- All bulk exports from `exports_kitchen_sink_bulk.txt`

### Using Profiles

```python
from screamingfrog.cli import get_export_profile

# Get profile contents
tabs, bulk = get_export_profile("kitchen_sink")
print(f"Tabs: {len(tabs)}")
print(f"Bulk exports: {len(bulk)}")

# Use with export_crawl
export_crawl(
    load_target="./crawl.seospider",
    export_dir="./exports",
    export_tabs=tabs,
    bulk_exports=bulk,
)
```

### Profile Files

Profiles are defined in text files in the package:

```
# exports_kitchen_sink_tabs.txt
Internal:All
Internal:HTML
Response Codes:All
Response Codes:Client Error (4xx)
Response Codes:Server Error (5xx)
Page Titles:All
Page Titles:Missing
...

# exports_kitchen_sink_bulk.txt
Bulk Export: Sitemaps
Bulk Export: All Links
...
```

---

## Loading .seospider Files

### Derby Mode (Default)

Convert `.seospider` to Derby database and use Derby backend:

```python
# Default: Derby mode with .dbseospider cache
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",
)

# Custom cache path
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",
    dbseospider_path="./cache/my-crawl.dbseospider",
)

# Reuse existing cache
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",
    dbseospider_overwrite=False,  # Skip if exists
)

# No cache (use ProjectInstanceData directly)
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",
    materialize_dbseospider=False,
)
```

### CSV Mode

Export to CSV and use CSV backend:

```python
# CSV mode with specific tabs
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_tabs=["Internal:All", "Page Titles:All"],
)

# CSV mode with kitchen_sink profile
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_profile="kitchen_sink",
)

# Custom export directory
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_dir="./my_exports",
)
```

---

## Storage Mode Enforcement

The library can temporarily force DB storage mode for `.seospider` files:

```python
# Force DB mode (default)
crawl = Crawl.load(
    "./crawl.seospider",
    ensure_db_mode=True,  # Sets storage.mode=DB in spider.config
)

# Skip DB mode enforcement
crawl = Crawl.load(
    "./crawl.seospider",
    ensure_db_mode=False,  # Use current storage mode
)
```

### How It Works

1. Reads `spider.config` from the crawl file
2. Temporarily sets `storage.mode=DB` if not already set
3. Loads the crawl via CLI
4. Restores original `spider.config` after loading

---

## Headless Operation

### Running Headless

```python
# Headless mode (default)
crawl = Crawl.load(
    "./crawl.seospider",
    headless=True,  # No GUI window
)

# With GUI (for debugging)
crawl = Crawl.load(
    "./crawl.seospider",
    headless=False,  # Show SF window
)
```

### Environment Variables for Headless

On Linux, you may need to configure a virtual display:

```bash
# Using Xvfb
export DISPLAY=:99
Xvfb :99 -screen 0 1024x768x24 &

# Or use xvfb-run
xvfb-run python my_script.py
```

---

## Overwrite Behavior

### Force Re-export

```python
# Force re-export (default)
crawl = Crawl.load(
    "./crawl.seospider",
    overwrite=True,  # Overwrite existing exports
)

# Skip if exists
crawl = Crawl.load(
    "./crawl.seospider",
    overwrite=False,  # Reuse existing exports
)

# Force export even if cache exists
crawl = Crawl.load(
    "./crawl.seospider",
    force_export=True,  # Always export fresh
)
```

---

## Advanced CLI Usage

### Direct CLI Function

```python
from screamingfrog.cli import run_cli

# Run arbitrary CLI command
result = run_cli([
    "--crawl", "https://example.com",
    "--headless",
    "--export-tabs", "Internal:All",
    "--output-folder", "./exports",
])
```

### Load and Export Separately

```python
from screamingfrog.cli import export_crawl
from screamingfrog import Crawl

# Export first
export_crawl(
    load_target="./crawl.seospider",
    export_dir="./exports",
    export_tabs=["Internal:All", "Response Codes:All"],
)

# Then load from exports
crawl = Crawl.from_exports("./exports")
```

---

## Error Handling

### CLI Not Found

```python
try:
    crawl = Crawl.load("./crawl.seospider")
except FileNotFoundError as e:
    print("Screaming Frog CLI not found")
    print("Install SF or set SCREAMINGFROG_CLI environment variable")
```

### Export Failures

```python
try:
    export_crawl(
        load_target="./nonexistent.seospider",
        export_dir="./exports",
        export_tabs=["Internal:All"],
    )
except Exception as e:
    print(f"Export failed: {e}")
```

### Timeout Handling

For large crawls, exports may take a long time:

```python
import subprocess

# Increase timeout (not directly exposed, but CLI honors subprocess timeout)
# Consider breaking into smaller export batches
```

---

## Performance Tips

### Export Only What You Need

```python
# Good: Export specific tabs
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_tabs=["Internal:All", "Page Titles:Missing"],
)

# Slower: Export everything
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_profile="kitchen_sink",  # Exports 50+ tabs
)
```

### Cache Exports

```python
# First run: exports to cache
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="csv",
    export_dir="./cache/exports",
)

# Later runs: reuse cache
crawl = Crawl.from_exports("./cache/exports")
```

### Use Derby for Speed

```python
# Derby is faster than CSV for queries
crawl = Crawl.load(
    "./crawl.seospider",
    seospider_backend="derby",  # Fast database queries
)
```

---

## Automation Example

### Scheduled Export Script

```python
#!/usr/bin/env python
"""Export crawl data on a schedule."""

import os
import datetime
from screamingfrog import Crawl

# Configuration
CRAWL_FILE = "./weekly_crawl.seospider"
EXPORT_BASE = "./exports"

def export_crawl_data():
    # Create dated export directory
    date_str = datetime.date.today().isoformat()
    export_dir = os.path.join(EXPORT_BASE, date_str)
    os.makedirs(export_dir, exist_ok=True)

    # Load and export
    crawl = Crawl.load(
        CRAWL_FILE,
        seospider_backend="csv",
        export_dir=export_dir,
        export_profile="kitchen_sink",
        headless=True,
    )

    # Generate summary
    total = crawl.internal.count()
    errors = crawl.tab("response_codes").filter(gui="Client Error (4xx)").count()

    print(f"Exported to: {export_dir}")
    print(f"Total pages: {total}")
    print(f"4xx errors: {errors}")

if __name__ == "__main__":
    export_crawl_data()
```

---

## Limitations

- **Requires SF CLI**: Must have Screaming Frog installed with CLI access
- **License**: Some features require a paid SF license
- **Platform**: CLI behavior varies by platform
- **Performance**: Large crawl exports can be slow
- **Headless**: May require display configuration on Linux
- **Concurrent**: Only one SF instance can run at a time
