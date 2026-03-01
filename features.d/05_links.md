# Inlinks and Outlinks

This document covers link graph traversal, link data structure, and link type labels.

---

## Overview

The library provides access to the link graph through the Derby backend, allowing you to traverse inbound and outbound links for any URL in the crawl.

**Backend Support:**

| Backend | Inlinks | Outlinks |
|---------|---------|----------|
| Derby | Yes | Yes |
| Hybrid | Yes | Yes |
| CSV | No | No |
| SQLite | No | No |
| CLI | No | No |

---

## Basic Link Traversal

### `crawl.inlinks(url)` - Inbound Links

Get all links pointing TO a URL:

```python
# Get all inlinks to a page
for link in crawl.inlinks("https://example.com/page"):
    print(f"{link.source} -> {link.destination}")
    print(f"  Anchor: {link.anchor_text}")
    print(f"  Type: {link.data.get('Link Type')}")
```

### `crawl.outlinks(url)` - Outbound Links

Get all links FROM a URL:

```python
# Get all outlinks from a page
for link in crawl.outlinks("https://example.com/page"):
    print(f"{link.source} -> {link.destination}")
    print(f"  Anchor: {link.anchor_text}")
```

---

## Link Data Model

### Link Object

```python
from dataclasses import dataclass
from typing import Any, Dict, Optional

@dataclass(frozen=True)
class Link:
    source: Optional[str]           # Source URL
    destination: Optional[str]      # Destination URL
    anchor_text: Optional[str]      # Link anchor text
    data: Dict[str, Any]            # Additional link metadata
```

### Primary Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `source` | `Optional[str]` | The URL where the link originates |
| `destination` | `Optional[str]` | The URL the link points to |
| `anchor_text` | `Optional[str]` | The visible text of the link |
| `data` | `dict` | Full link metadata dictionary |

### Data Dictionary Fields

The `data` dictionary contains additional metadata from the Derby database:

| Field | Description |
|-------|-------------|
| `Rel` | Link rel attribute value (e.g., "nofollow") |
| `NoFollow` | Boolean flag for nofollow links |
| `Alt Text` | Image alt text (for image links) |
| `Link Path` | DOM path to the link element |
| `Link Position` | Position of link on the page |
| `Status Code` | HTTP status code of the destination |
| `Content Type` | Content type of the destination |
| `Size` | Size of the destination resource |
| `Link Type` | Type label (see Link Types below) |
| `Href Lang` | Hreflang value (for hreflang links) |

---

## Link Types

Links are classified by type using numeric IDs in the Derby database. The library maps these to human-readable labels:

| ID | Link Type |
|----|-----------|
| 1 | Hyperlink |
| 6 | Canonical |
| 8 | Rel Prev |
| 10 | Rel Next |
| 12 | Hreflang (HTTP) |
| 13 | Hreflang |

### Filtering by Link Type

```python
# Get only hyperlinks
for link in crawl.inlinks("https://example.com/page"):
    if link.data.get("Link Type") == "Hyperlink":
        print(f"Hyperlink: {link.source}")

# Get canonical links
for link in crawl.inlinks("https://example.com/page"):
    if link.data.get("Link Type") == "Canonical":
        print(f"Canonical from: {link.source}")

# Get hreflang links
for link in crawl.inlinks("https://example.com/page"):
    if "Hreflang" in link.data.get("Link Type", ""):
        print(f"Hreflang: {link.source} ({link.data.get('Href Lang')})")
```

---

## Common Use Cases

### Find Orphan Pages

Pages with no internal inlinks are hard for search engines to discover:

```python
orphans = []
for page in crawl.internal.filter(content_type="text/html"):
    inlinks = list(crawl.inlinks(page.address))
    internal_inlinks = [l for l in inlinks if l.source and "example.com" in l.source]
    if len(internal_inlinks) == 0:
        orphans.append(page.address)

print(f"Found {len(orphans)} orphan pages")
for url in orphans[:10]:
    print(f"  {url}")
```

### Analyze Internal Link Distribution

```python
from collections import Counter

inlink_counts = Counter()
for page in crawl.internal.filter(content_type="text/html"):
    inlinks = list(crawl.inlinks(page.address))
    inlink_counts[page.address] = len(inlinks)

# Top 10 most linked pages
print("Most linked pages:")
for url, count in inlink_counts.most_common(10):
    print(f"  {count}: {url}")

# Pages with fewest links
print("\nLeast linked pages:")
for url, count in inlink_counts.most_common()[-10:]:
    print(f"  {count}: {url}")
```

### Find Broken Link Sources

Identify pages linking to broken URLs:

```python
# Find all 4xx pages and their inlinks
for page in crawl.internal.filter(status_code=404):
    inlinks = list(crawl.inlinks(page.address))
    if inlinks:
        print(f"\n404: {page.address}")
        print(f"  Linked from {len(inlinks)} pages:")
        for link in inlinks[:5]:
            print(f"    - {link.source}")
            print(f"      Anchor: {link.anchor_text}")
```

### Audit Canonical Links

```python
# Find pages with canonical links
for page in crawl.internal.filter(content_type="text/html"):
    outlinks = list(crawl.outlinks(page.address))
    canonicals = [l for l in outlinks if l.data.get("Link Type") == "Canonical"]

    if canonicals:
        for canon in canonicals:
            if canon.destination != page.address:
                print(f"Non-self canonical: {page.address}")
                print(f"  Points to: {canon.destination}")
```

### Find Redirect Chains via Links

```python
# Find pages with redirects as outlink destinations
for page in crawl.internal.filter(content_type="text/html"):
    outlinks = list(crawl.outlinks(page.address))

    for link in outlinks:
        if link.data.get("Status Code") in [301, 302, 307, 308]:
            print(f"Link to redirect: {page.address}")
            print(f"  -> {link.destination} ({link.data.get('Status Code')})")
```

### Analyze Hreflang Implementation

```python
# Audit hreflang links
for page in crawl.internal.filter(content_type="text/html"):
    outlinks = list(crawl.outlinks(page.address))
    hreflangs = [l for l in outlinks if "Hreflang" in l.data.get("Link Type", "")]

    if hreflangs:
        print(f"\n{page.address}")
        for hl in hreflangs:
            lang = hl.data.get("Href Lang", "?")
            print(f"  {lang}: {hl.destination}")
```

### Find Nofollow Links

```python
# Find pages with nofollow outlinks
for page in crawl.internal.filter(content_type="text/html"):
    outlinks = list(crawl.outlinks(page.address))
    nofollows = [l for l in outlinks if l.data.get("NoFollow")]

    if nofollows:
        print(f"{page.address} has {len(nofollows)} nofollow links")
```

---

## Derby Database Structure

Links are stored in the Derby database across multiple tables:

### APP.LINKS Table

| Column | Description |
|--------|-------------|
| `SRC_ID` | Foreign key to source URL in UNIQUE_URLS |
| `DST_ID` | Foreign key to destination URL in UNIQUE_URLS |
| `ANCHOR` | Anchor text |
| `LINK_TYPE` | Link type ID (1, 6, 8, 10, 12, 13) |
| `REL` | Rel attribute value |
| `NO_FOLLOW` | Nofollow flag |
| `ALT_TEXT` | Image alt text |
| `LINK_PATH` | DOM path |
| `HREF_LANG` | Hreflang value |

### APP.UNIQUE_URLS Table

| Column | Description |
|--------|-------------|
| `ID` | Unique URL identifier |
| `ENCODED_URL` | The URL string |

### APP.URLS Table

| Column | Description |
|--------|-------------|
| `ENCODED_URL` | The URL string |
| `RESPONSE_CODE` | HTTP status code |
| `CONTENT_TYPE` | MIME content type |
| `PAGE_SIZE` | Size in bytes |

---

## Filtered Inlinks Tabs

The library maps filtered inlinks CSV tabs to Derby queries with richer link details:

| Tab | Description |
|-----|-------------|
| `client_error_(4xx)_inlinks.csv` | Inlinks to 4xx pages |
| `server_error_(5xx)_inlinks.csv` | Inlinks to 5xx pages |
| `redirection_(3xx)_inlinks.csv` | Inlinks to 3xx pages |
| `no_response_inlinks.csv` | Inlinks to no-response pages |

These tabs provide link details including source URL, anchor text, and link metadata.

---

## Raw SQL Access to Links

For advanced queries, use the SQL escape hatch:

```python
# Count links by type
for row in crawl.sql("""
    SELECT LINK_TYPE, COUNT(*) as cnt
    FROM APP.LINKS
    GROUP BY LINK_TYPE
    ORDER BY cnt DESC
"""):
    print(f"Type {row['LINK_TYPE']}: {row['cnt']} links")

# Find pages with most outlinks
for row in crawl.sql("""
    SELECT u.ENCODED_URL, COUNT(*) as outlink_count
    FROM APP.LINKS l
    JOIN APP.UNIQUE_URLS u ON l.SRC_ID = u.ID
    GROUP BY u.ENCODED_URL
    ORDER BY outlink_count DESC
    FETCH FIRST 10 ROWS ONLY
"""):
    print(f"{row['outlink_count']}: {row['ENCODED_URL']}")

# Find broken link sources with details
for row in crawl.sql("""
    SELECT
        src.ENCODED_URL as source,
        dst.ENCODED_URL as destination,
        l.ANCHOR,
        urls.RESPONSE_CODE
    FROM APP.LINKS l
    JOIN APP.UNIQUE_URLS src ON l.SRC_ID = src.ID
    JOIN APP.UNIQUE_URLS dst ON l.DST_ID = dst.ID
    JOIN APP.URLS urls ON dst.ENCODED_URL = urls.ENCODED_URL
    WHERE urls.RESPONSE_CODE BETWEEN 400 AND 499
"""):
    print(f"{row['source']} -> {row['destination']} ({row['RESPONSE_CODE']})")
```

---

## Performance Considerations

### Large Crawls

For crawls with millions of links:

- Iterate links lazily (generators) rather than collecting to lists
- Use SQL queries for aggregations instead of Python loops
- Filter by link type in SQL when possible

### Memory Usage

```python
# Good: Lazy iteration
for link in crawl.inlinks(url):
    process(link)

# Avoid for large result sets:
all_links = list(crawl.inlinks(url))  # Loads all into memory
```

### Batching

For bulk operations, consider batching:

```python
# Process inlinks in batches
urls = [p.address for p in crawl.internal]
batch_size = 100

for i in range(0, len(urls), batch_size):
    batch = urls[i:i+batch_size]
    for url in batch:
        inlinks = list(crawl.inlinks(url))
        # Process batch
```

---

## Limitations

- **CSV/SQLite backends**: No link graph support; `inlinks()` and `outlinks()` raise `NotImplementedError`
- **External links**: Link data for external URLs may be incomplete
- **JavaScript-rendered links**: Only available if crawled with JavaScript rendering
- **Link deduplication**: The library uses UNIQUE_URLS for deduplication, but duplicate links between the same src/dst may exist with different anchor text
- **HTTP header links**: Canonical and hreflang from HTTP headers are parsed from response blobs
