# Crawl Diff

This document covers crawl-over-crawl comparison, change detection, and diff output structures.

---

## Overview

The library provides crawl comparison capabilities to detect changes between two crawls. This is useful for:

- Monitoring site changes over time
- Detecting unintended modifications after deployments
- Tracking SEO-relevant changes (titles, redirects, indexability)
- Migration audits

---

## Basic Usage

### Comparing Two Crawls

```python
from screamingfrog import Crawl

# Load both crawls
old = Crawl.load("./old.dbseospider")
new = Crawl.load("./new.dbseospider")

# Compare (new vs old)
diff = new.compare(old)

# Access results
print(f"Added pages: {len(diff.added_pages)}")
print(f"Removed pages: {len(diff.removed_pages)}")
print(f"Status changes: {len(diff.status_changes)}")
print(f"Title changes: {len(diff.title_changes)}")
print(f"Redirect changes: {len(diff.redirect_changes)}")
print(f"Field changes: {len(diff.field_changes)}")
```

---

## CrawlDiff Data Model

### CrawlDiff Object

```python
from dataclasses import dataclass
from typing import List

@dataclass(frozen=True)
class CrawlDiff:
    added_pages: List[str]                    # URLs only in new crawl
    removed_pages: List[str]                  # URLs only in old crawl
    status_changes: List[StatusChange]        # HTTP status changes
    title_changes: List[TitleChange]          # Title tag changes
    redirect_changes: List[RedirectChange]    # Redirect target changes
    field_changes: List[FieldChange]          # Other field changes
```

### StatusChange

Tracks HTTP status code changes:

```python
@dataclass(frozen=True)
class StatusChange:
    url: str                        # Page URL
    old_status: Optional[int]       # Previous status code
    new_status: Optional[int]       # Current status code
```

### TitleChange

Tracks title tag changes:

```python
@dataclass(frozen=True)
class TitleChange:
    url: str                        # Page URL
    old_title: Optional[str]        # Previous title
    new_title: Optional[str]        # Current title
```

### RedirectChange

Tracks redirect target and type changes:

```python
@dataclass(frozen=True)
class RedirectChange:
    url: str                        # Page URL
    old_target: Optional[str]       # Previous redirect target
    new_target: Optional[str]       # Current redirect target
    old_type: Optional[str]         # Previous redirect type (301, 302, etc.)
    new_type: Optional[str]         # Current redirect type
```

### FieldChange

Tracks changes to other tracked fields:

```python
@dataclass(frozen=True)
class FieldChange:
    url: str                        # Page URL
    field: str                      # Field name (e.g., "Canonical", "H1-1")
    old_value: Optional[str]        # Previous value
    new_value: Optional[str]        # Current value
```

---

## Default Field Groups

The diff automatically tracks changes to these field groups:

| Group | Column Candidates |
|-------|------------------|
| Canonical | `Canonical Link Element 1` |
| Canonical Status | `Canonical Link Element 1 Is Self-Referencing`, `Canonical Link Element 1 Target Is Non-Indexable`, `Canonical Link Element 1 From HTTP Header` |
| Meta Description | `Meta Description 1` |
| Meta Keywords | `Meta Keywords 1` |
| Meta Refresh | `Meta Refresh`, `Meta Refresh Target` |
| H1-1 | `H1-1` |
| H2-1 | `H2-1` |
| H3-1 | `H3-1` |
| Word Count | `Word Count` |
| Indexability | `Indexability` |
| Indexability Status | `Indexability Status` |
| Meta Robots | `Meta Robots 1` |
| X-Robots-Tag | `X-Robots-Tag 1` |
| Directives Summary | Combined meta robots + X-Robots-Tag tokens |

---

## Customization

### Custom Title Fields

Override which columns are compared for title changes:

```python
diff = new.compare(
    old,
    title_fields=("Title 1", "Title", "Page Title"),
)
```

The diff will use the first matching column found in the crawl data.

### Custom Redirect Fields

Override columns used for redirect detection:

```python
diff = new.compare(
    old,
    redirect_fields=("Redirect URL", "Redirect URI", "Location"),
    redirect_type_fields=("Redirect Type", "Status Code"),
)
```

### Custom Field Groups

Add or override field groups to track:

```python
diff = new.compare(
    old,
    field_groups={
        # Override H2-1 with multiple candidates
        "H2-1": ("H2-1", "H2_1"),
        # Add custom extraction tracking
        "Price": ("Price", "product_price"),
        # Track additional meta tags
        "OG:Title": ("og:title", "Open Graph Title"),
    },
)
```

---

## Working with Diff Results

### Iterate Added Pages

```python
print("New pages added:")
for url in diff.added_pages[:10]:
    print(f"  + {url}")
```

### Iterate Removed Pages

```python
print("Pages removed:")
for url in diff.removed_pages[:10]:
    print(f"  - {url}")
```

### Analyze Status Changes

```python
# Find pages that went from 200 to 404
broken = [
    c for c in diff.status_changes
    if c.old_status == 200 and c.new_status == 404
]
print(f"Pages now broken: {len(broken)}")
for change in broken[:10]:
    print(f"  {change.url}: {change.old_status} -> {change.new_status}")

# Find pages that were fixed
fixed = [
    c for c in diff.status_changes
    if c.old_status in [404, 500] and c.new_status == 200
]
print(f"Pages fixed: {len(fixed)}")
```

### Analyze Title Changes

```python
# Find significant title changes
for change in diff.title_changes[:10]:
    print(f"Title changed: {change.url}")
    print(f"  Old: {change.old_title}")
    print(f"  New: {change.new_title}")
```

### Analyze Redirect Changes

```python
# Find redirect target changes
for change in diff.redirect_changes[:10]:
    print(f"Redirect changed: {change.url}")
    print(f"  Old: {change.old_target} ({change.old_type})")
    print(f"  New: {change.new_target} ({change.new_type})")

# Find new redirects
new_redirects = [
    c for c in diff.redirect_changes
    if c.old_target is None and c.new_target is not None
]
print(f"New redirects: {len(new_redirects)}")
```

### Analyze Field Changes

```python
# Group changes by field
from collections import defaultdict

by_field = defaultdict(list)
for change in diff.field_changes:
    by_field[change.field].append(change)

for field, changes in by_field.items():
    print(f"{field}: {len(changes)} changes")

# Find indexability changes
indexability_changes = [
    c for c in diff.field_changes
    if c.field == "Indexability"
]
for change in indexability_changes[:10]:
    print(f"{change.url}: {change.old_value} -> {change.new_value}")
```

---

## Common Diff Patterns

### Migration Audit

```python
# Load pre and post migration crawls
old = Crawl.load("./pre_migration.dbseospider")
new = Crawl.load("./post_migration.dbseospider")

diff = new.compare(old)

# Check for lost pages
if diff.removed_pages:
    print(f"WARNING: {len(diff.removed_pages)} pages no longer exist!")
    for url in diff.removed_pages[:10]:
        print(f"  LOST: {url}")

# Check for broken pages
broken = [c for c in diff.status_changes if c.new_status and c.new_status >= 400]
if broken:
    print(f"WARNING: {len(broken)} pages now return errors!")

# Check redirect targets
for change in diff.redirect_changes:
    if change.old_target != change.new_target:
        print(f"Redirect changed: {change.url}")
        print(f"  Was: {change.old_target}")
        print(f"  Now: {change.new_target}")
```

### Weekly Monitoring

```python
# Compare weekly crawls
this_week = Crawl.load("./crawl_week_52.dbseospider")
last_week = Crawl.load("./crawl_week_51.dbseospider")

diff = this_week.compare(last_week)

# Summary report
print("Weekly Crawl Diff Summary")
print("=" * 40)
print(f"Pages added:    {len(diff.added_pages)}")
print(f"Pages removed:  {len(diff.removed_pages)}")
print(f"Status changes: {len(diff.status_changes)}")
print(f"Title changes:  {len(diff.title_changes)}")
print(f"Field changes:  {len(diff.field_changes)}")

# Alert on significant changes
if len(diff.removed_pages) > 100:
    print("ALERT: Large number of pages removed!")

error_count = sum(1 for c in diff.status_changes if c.new_status and c.new_status >= 400)
if error_count > 10:
    print(f"ALERT: {error_count} pages now have errors!")
```

### Title Change Report

```python
# Generate title change report
print("Title Changes")
print("=" * 60)

for change in diff.title_changes:
    print(f"\nURL: {change.url}")
    print(f"  Before: {change.old_title or '(none)'}")
    print(f"  After:  {change.new_title or '(none)'}")

    # Check for common issues
    if not change.new_title:
        print("  WARNING: Title removed!")
    elif change.old_title and len(change.new_title) < len(change.old_title) * 0.5:
        print("  WARNING: Title significantly shortened!")
```

### Canonical Change Report

```python
# Track canonical changes specifically
canonical_changes = [
    c for c in diff.field_changes
    if c.field == "Canonical"
]

print(f"Canonical Changes: {len(canonical_changes)}")
for change in canonical_changes[:20]:
    print(f"\n{change.url}")
    print(f"  Old canonical: {change.old_value or 'None'}")
    print(f"  New canonical: {change.new_value or 'None'}")
```

---

## Export Diff Results

### To CSV

```python
import csv

# Export status changes to CSV
with open("status_changes.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["URL", "Old Status", "New Status"])
    for change in diff.status_changes:
        writer.writerow([change.url, change.old_status, change.new_status])

# Export all field changes
with open("field_changes.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["URL", "Field", "Old Value", "New Value"])
    for change in diff.field_changes:
        writer.writerow([change.url, change.field, change.old_value, change.new_value])
```

### To JSON

```python
import json
from dataclasses import asdict

# Convert diff to JSON-serializable format
diff_data = {
    "added_pages": diff.added_pages,
    "removed_pages": diff.removed_pages,
    "status_changes": [asdict(c) for c in diff.status_changes],
    "title_changes": [asdict(c) for c in diff.title_changes],
    "redirect_changes": [asdict(c) for c in diff.redirect_changes],
    "field_changes": [asdict(c) for c in diff.field_changes],
}

with open("diff_report.json", "w") as f:
    json.dump(diff_data, f, indent=2)
```

### To DataFrame (pandas)

```python
import pandas as pd
from dataclasses import asdict

# Status changes DataFrame
status_df = pd.DataFrame([asdict(c) for c in diff.status_changes])
print(status_df.head())

# Field changes DataFrame
field_df = pd.DataFrame([asdict(c) for c in diff.field_changes])
print(field_df.head())

# Pivot field changes
pivot = field_df.pivot(index="url", columns="field", values="new_value")
print(pivot.head())
```

---

## Redirect Detection

The diff uses best-effort redirect detection from multiple sources:

1. **Redirect URL column** - Direct redirect target from SF
2. **Redirect Type column** - HTTP status code for redirect
3. **Meta Refresh** - Meta refresh redirect target
4. **Location header** - From HTTP response headers (parsed from blobs)

### Redirect Column Priority

```python
# Default redirect field candidates (in order of priority)
redirect_fields = (
    "Redirect URL",
    "Redirect URI",
    "Meta Refresh Target",
    "Location",
)

redirect_type_fields = (
    "Redirect Type",
    "Status Code",
)
```

---

## Performance Considerations

### Memory Usage

The diff loads all internal pages from both crawls into memory for comparison. For very large crawls:

```python
# For large crawls, consider filtering first
old_internal = {p.address: p for p in old.internal.filter(content_type="text/html")}
new_internal = {p.address: p for p in new.internal.filter(content_type="text/html")}
```

### Iteration

Results are stored as lists, not generators. For large diffs, iterate rather than loading all:

```python
# Good: Iterate
for change in diff.status_changes:
    process(change)

# Avoid unnecessary copies
changes_copy = list(diff.status_changes)  # Already a list
```

---

## Scope and Limitations

### Current Scope

- **Internal pages only** - External URLs are not compared
- **Field-level comparison** - Binary (changed/not changed), no similarity scoring
- **Case-sensitive** - String comparisons are case-sensitive
- **URL matching** - URLs must match exactly (no normalization)

### Not Supported

- External URL comparison
- Content similarity scoring (e.g., levenshtein distance)
- Visual diff
- Link graph diff (inlinks/outlinks changes)
- Structured data diff
- Performance metrics diff (load times, sizes)

### Future Considerations

- URL normalization options
- Custom comparison functions
- Link graph diff
- Structured data changes
- HTML content similarity
