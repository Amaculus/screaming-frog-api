# DB Packaging and Project Helpers

This document covers `.dbseospider` packing/unpacking, project discovery, and DB-mode utilities.

---

## Overview

Screaming Frog stores crawl data in different formats:

- **`.seospider`** - Memory/file-based crawl archive
- **`.dbseospider`** - Derby database archive (zipped DB folder)
- **DB-mode project** - Live Derby database in `ProjectInstanceData`

The library provides utilities to convert between these formats.

---

## .dbseospider File Format

A `.dbseospider` file is a ZIP archive containing a Derby database folder:

```
crawl.dbseospider
├── seg0/
│   ├── c10.dat
│   ├── c20.dat
│   └── ...
├── log/
│   └── log.ctrl
├── service.properties
└── README_DO_NOT_TOUCH_FILES.txt
```

---

## Packing Utilities

### `pack_dbseospider()` - Create .dbseospider from Project

Pack a ProjectInstanceData crawl directory into a `.dbseospider` file:

```python
from screamingfrog.packaging import pack_dbseospider

# Pack from project directory
pack_dbseospider(
    project_dir="~/.ScreamingFrogSEOSpider/ProjectInstanceData/138edb21-61d0-41cd-9e9b-725b592a471c",
    output_file="./crawl.dbseospider",
)
```

### `pack_dbseospider_from_db_id()` - Pack by Crawl ID

Pack using the database crawl ID:

```python
from screamingfrog.packaging import pack_dbseospider_from_db_id

# Pack by crawl ID
pack_dbseospider_from_db_id(
    db_id="138edb21-61d0-41cd-9e9b-725b592a471c",
    output_file="./crawl.dbseospider",
)

# With custom project root
pack_dbseospider_from_db_id(
    db_id="138edb21-61d0-41cd-9e9b-725b592a471c",
    output_file="./crawl.dbseospider",
    project_root=r"C:\Custom\ProjectInstanceData",
)
```

### `export_dbseospider_from_seospider()` - Convert .seospider to .dbseospider

Convert a `.seospider` file to `.dbseospider`:

```python
from screamingfrog.packaging import export_dbseospider_from_seospider

# Convert .seospider to .dbseospider
export_dbseospider_from_seospider(
    crawl_path="./crawl.seospider",
    output_file="./crawl.dbseospider",
)

# With options
export_dbseospider_from_seospider(
    crawl_path="./crawl.seospider",
    output_file="./crawl.dbseospider",
    cli_path=r"C:\Custom\ScreamingFrogSEOSpiderCli.exe",
    headless=True,
)
```

---

## Unpacking Utilities

### `unpack_dbseospider()` - Extract .dbseospider

Extract a `.dbseospider` file to a directory:

```python
from screamingfrog.packaging import unpack_dbseospider

# Extract to directory
unpack_dbseospider(
    dbseospider_file="./crawl.dbseospider",
    output_dir="./extracted_crawl",
)

# The extracted directory can be used directly:
from screamingfrog import Crawl
crawl = Crawl.from_derby("./extracted_crawl")
```

---

## Project Discovery

### `find_project_dir()` - Locate Crawl by ID

Find a ProjectInstanceData crawl directory by its database ID:

```python
from screamingfrog.packaging import find_project_dir

# Find project directory
project_dir = find_project_dir("138edb21-61d0-41cd-9e9b-725b592a471c")
print(project_dir)
# ~/.ScreamingFrogSEOSpider/ProjectInstanceData/138edb21-61d0-41cd-9e9b-725b592a471c

# With custom root
project_dir = find_project_dir(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    project_root=r"C:\Custom\ProjectInstanceData",
)
```

### `resolve_project_root()` - Get ProjectInstanceData Location

Resolve the ProjectInstanceData root directory:

```python
from screamingfrog.packaging import resolve_project_root

# Auto-detect
root = resolve_project_root()
print(root)  # Platform-specific default

# Explicit
root = resolve_project_root(r"C:\Custom\ProjectInstanceData")
print(root)  # C:\Custom\ProjectInstanceData
```

### Resolution Order

1. Explicit `project_root` parameter
2. `SCREAMINGFROG_PROJECT_DIR` environment variable
3. Platform-specific defaults:
   - **Windows**: `%APPDATA%\.ScreamingFrogSEOSpider\ProjectInstanceData`
   - **macOS/Linux**: `~/.ScreamingFrogSEOSpider/ProjectInstanceData`

---

## Loading from DB ID

### Direct Loading

```python
from screamingfrog import Crawl

# Load by database ID
crawl = Crawl.load("138edb21-61d0-41cd-9e9b-725b592a471c")

# Force source type
crawl = Crawl.load(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    source_type="db_id",
)

# With custom project root
crawl = Crawl.load(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    source_type="db_id",
    project_root=r"C:\Custom\ProjectInstanceData",
)
```

### `Crawl.from_db_id()`

```python
from screamingfrog import Crawl

crawl = Crawl.from_db_id(
    "138edb21-61d0-41cd-9e9b-725b592a471c",
    project_root=None,              # Auto-detect
    backend="derby",                # "derby" or "csv"
    csv_fallback=True,              # Enable Hybrid backend
    csv_fallback_profile="kitchen_sink",
)
```

---

## .seospider to Derby Workflow

### Step 1: Load .seospider to DB Mode

```python
from screamingfrog.packaging import load_seospider_db_project

# Load .seospider into DB mode via CLI
project_dir = load_seospider_db_project(
    crawl_path="./crawl.seospider",
    cli_path=None,                  # Auto-detect
    ensure_db_mode=True,            # Force DB storage
    headless=True,
)

print(f"Project loaded to: {project_dir}")
```

### Step 2: Pack to .dbseospider

```python
from screamingfrog.packaging import pack_dbseospider

# Pack the loaded project
pack_dbseospider(
    project_dir=project_dir,
    output_file="./crawl.dbseospider",
)
```

### Or Use Combined Function

```python
from screamingfrog.packaging import export_dbseospider_from_seospider

# One-step conversion
export_dbseospider_from_seospider(
    crawl_path="./crawl.seospider",
    output_file="./crawl.dbseospider",
)
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SCREAMINGFROG_PROJECT_DIR` | ProjectInstanceData root | Platform default |
| `SCREAMINGFROG_CLI` | CLI executable path | Auto-detect |

### Setting Environment Variables

**Windows (PowerShell):**
```powershell
$env:SCREAMINGFROG_PROJECT_DIR = "C:\Custom\ProjectInstanceData"
$env:SCREAMINGFROG_CLI = "C:\Program Files\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe"
```

**macOS/Linux:**
```bash
export SCREAMINGFROG_PROJECT_DIR="$HOME/.ScreamingFrogSEOSpider/ProjectInstanceData"
export SCREAMINGFROG_CLI="/Applications/Screaming Frog SEO Spider.app/Contents/MacOS/ScreamingFrogSEOSpider"
```

---

## Common Workflows

### Archive a Live Crawl

```python
from screamingfrog.packaging import pack_dbseospider_from_db_id
import datetime

# Pack current crawl to dated archive
db_id = "138edb21-61d0-41cd-9e9b-725b592a471c"
date_str = datetime.date.today().isoformat()
output_file = f"./archives/crawl_{date_str}.dbseospider"

pack_dbseospider_from_db_id(db_id, output_file)
print(f"Archived to: {output_file}")
```

### Share a Crawl

```python
from screamingfrog.packaging import export_dbseospider_from_seospider

# Convert .seospider to shareable .dbseospider
export_dbseospider_from_seospider(
    crawl_path="./my_crawl.seospider",
    output_file="./shared/my_crawl.dbseospider",
)
```

### Batch Convert Multiple Crawls

```python
import os
from screamingfrog.packaging import export_dbseospider_from_seospider

# Convert all .seospider files in a directory
input_dir = "./seospider_files"
output_dir = "./dbseospider_files"

os.makedirs(output_dir, exist_ok=True)

for filename in os.listdir(input_dir):
    if filename.endswith(".seospider"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename.replace(".seospider", ".dbseospider"))

        print(f"Converting: {filename}")
        export_dbseospider_from_seospider(input_path, output_path)
        print(f"Created: {output_path}")
```

### Restore from Archive

```python
from screamingfrog.packaging import unpack_dbseospider
from screamingfrog import Crawl

# Unpack archive
unpack_dbseospider(
    dbseospider_file="./archives/crawl_2024-01-15.dbseospider",
    output_dir="./restored_crawl",
)

# Load and analyze
crawl = Crawl.from_derby("./restored_crawl")
print(f"Pages: {crawl.internal.count()}")
```

---

## File Size Considerations

### .dbseospider Size

The `.dbseospider` file size depends on:

- Number of URLs crawled
- Crawl configuration (JS rendering, etc.)
- Data collected (structured data, etc.)

Typical sizes:
- Small crawl (1K URLs): 5-20 MB
- Medium crawl (50K URLs): 100-500 MB
- Large crawl (500K URLs): 1-5 GB

### Compression

`.dbseospider` files are ZIP archives with default compression. For additional compression:

```python
import shutil

# Further compress with gzip
shutil.make_archive(
    "crawl_compressed",
    "gztar",
    root_dir="./extracted_crawl",
)
```

---

## Error Handling

### Project Not Found

```python
from screamingfrog.packaging import find_project_dir

try:
    project_dir = find_project_dir("nonexistent-id")
except FileNotFoundError:
    print("Crawl not found in ProjectInstanceData")
```

### CLI Required

```python
from screamingfrog.packaging import export_dbseospider_from_seospider

try:
    export_dbseospider_from_seospider(
        "./crawl.seospider",
        "./crawl.dbseospider",
    )
except FileNotFoundError:
    print("Screaming Frog CLI not found")
```

### Invalid Archive

```python
from screamingfrog.packaging import unpack_dbseospider

try:
    unpack_dbseospider("./invalid.dbseospider", "./output")
except Exception as e:
    print(f"Invalid archive: {e}")
```

---

## Limitations

- **CLI required**: `.seospider` conversion requires Screaming Frog CLI
- **License**: Some operations may require SF license
- **Java required**: Derby operations require Java runtime
- **Disk space**: Unpacking large crawls requires significant disk space
- **Concurrent access**: Don't modify ProjectInstanceData while SF is running
