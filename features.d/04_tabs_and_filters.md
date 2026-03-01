# Tabs and GUI Filters

This document covers tab access, GUI-style filters, and column metadata.

---

## Tab Access Overview

The library provides access to Screaming Frog tabs through two primary methods:

1. **Typed Internal View** - `crawl.internal` returns `InternalPage` objects
2. **Generic Tab View** - `crawl.tab(name)` returns dict rows for any tab

---

## TabView API

### Basic Tab Access

```python
# Access any tab by name
for row in crawl.tab("response_codes_all"):
    print(row["Address"], row["Status Code"])

# Access by normalized name (all equivalent)
crawl.tab("Page Titles")          # Display name
crawl.tab("page_titles")          # Normalized
crawl.tab("page_titles_all")      # With suffix
crawl.tab("page_titles_all.csv")  # Full filename
```

### Filtering

```python
# Column filters
for row in crawl.tab("internal_all").filter(status_code=404):
    print(row["Address"])

# Column filter with list (IN clause)
for row in crawl.tab("internal_all").filter(status_code=[404, 500]):
    print(row["Address"])

# GUI filters
for row in crawl.tab("page_titles").filter(gui="Missing"):
    print(row["Address"])

# Multiple GUI filters
for row in crawl.tab("page_titles").filter(gui_filters=["Missing", "Duplicate"]):
    print(row["Address"])

# Combined column and GUI filters
for row in crawl.tab("page_titles").filter(gui="Duplicate", indexability="Indexable"):
    print(row["Address"])
```

### Counting

```python
# Count all rows
total = crawl.tab("page_titles").count()

# Count with filters
missing = crawl.tab("page_titles").filter(gui="Missing").count()
```

### Chaining

```python
# Filters can be chained (they're immutable views)
view = crawl.tab("internal_all")
html_view = view.filter(content_type="text/html")
errors = html_view.filter(status_code=404)
```

---

## Tab Metadata Helpers

### `crawl.tabs` - List Available Tabs

```python
tabs = crawl.tabs
print(tabs)  # ['internal_all.csv', 'response_codes_all.csv', ...]
```

### `crawl.tab_filters(name)` - List GUI Filters

```python
filters = crawl.tab_filters("Page Titles")
print(filters)  # ["All", "Missing", "Duplicate", "Over X Characters", ...]
```

### `crawl.tab_filter_defs(name)` - Full Filter Definitions

```python
filter_defs = crawl.tab_filter_defs("Page Titles")
for filt in filter_defs:
    print(f"{filt.name}: {filt.description}")
    print(f"  SQL: {filt.sql_where}")
    print(f"  Join: {filt.join_table}")
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

## Filter Implementation by Backend

### CSV Backend

- GUI filters resolved by selecting matching CSV files
- Example: `gui="Missing"` for Page Titles → `page_titles_missing.csv`
- Column filters applied via row iteration (simple equality)
- Filter availability depends on exported files

### Derby Backend

- GUI filters implemented as SQL WHERE clauses
- Some filters require JOIN with auxiliary tables
- High performance via database queries
- Falls back to CSV if filter not implemented

### SQLite Backend

- Limited GUI filter support
- Supports `Missing` filter for `page_titles` and `meta_description`
- Column filters via SQL WHERE clauses

### Hybrid Backend

- Tries Derby first
- Falls back to CSV when:
  - GUI filter has no SQL implementation
  - Mapped column resolves to NULL
  - Tab has no Derby mapping

---

## Complete GUI Filter Reference

### Response Codes Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All URLs with a response code | (no filter) |
| Blocked by Robots.txt | URLs blocked by robots.txt | `BLOCKED_BY_ROBOTS_TXT = 1` |
| Blocked Resource | Resources blocked by robots.txt | `BLOCKED_BY_ROBOTS_TXT = 1 AND LOADED_AS_A_RESOURCE = 1` |
| No Response | No response received | `RESPONSE_CODE IS NULL OR RESPONSE_CODE = 0` |
| Success (2xx) | HTTP 2xx responses | `RESPONSE_CODE BETWEEN 200 AND 299` |
| Redirection (3xx) | HTTP 3xx responses | `RESPONSE_CODE BETWEEN 300 AND 399` |
| Redirection (JavaScript) | JavaScript redirects | TODO |
| Redirection (Meta Refresh) | Meta refresh redirects | `NUM_METAREFRESH > 0` |
| Client Error (4xx) | HTTP 4xx responses | `RESPONSE_CODE BETWEEN 400 AND 499` |
| Server Error (5xx) | HTTP 5xx responses | `RESPONSE_CODE BETWEEN 500 AND 599` |

**Internal Subset:**

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| Internal All | Internal URLs with any response code | `IS_INTERNAL = 1` |
| Internal Blocked by Robots.txt | Internal URLs blocked by robots.txt | `IS_INTERNAL = 1 AND BLOCKED_BY_ROBOTS_TXT = 1` |
| Internal No Response | Internal URLs with no response | `IS_INTERNAL = 1 AND (RESPONSE_CODE IS NULL OR RESPONSE_CODE = 0)` |
| Internal Success (2xx) | Internal URLs with 2xx responses | `IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 200 AND 299` |
| Internal Redirection (3xx) | Internal URLs with 3xx responses | `IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 300 AND 399` |
| Internal Redirection (Meta Refresh) | Internal meta refresh redirects | `IS_INTERNAL = 1 AND NUM_METAREFRESH > 0` |
| Internal Redirect Chain | Internal redirect chains | TODO |
| Internal Redirect Loop | Internal redirect loops | TODO |
| Internal Client Error (4xx) | Internal URLs with 4xx responses | `IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 400 AND 499` |
| Internal Server Error (5xx) | Internal URLs with 5xx responses | `IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 500 AND 599` |

**External Subset:**

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| External All | External URLs with any response code | `IS_INTERNAL = 0` |
| External Blocked by Robots.txt | External URLs blocked by robots.txt | `IS_INTERNAL = 0 AND BLOCKED_BY_ROBOTS_TXT = 1` |
| External No Response | External URLs with no response | `IS_INTERNAL = 0 AND (RESPONSE_CODE IS NULL OR RESPONSE_CODE = 0)` |
| External Success (2xx) | External URLs with 2xx responses | `IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 200 AND 299` |
| External Redirection (3xx) | External URLs with 3xx responses | `IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 300 AND 399` |
| External Redirection (Meta Refresh) | External meta refresh redirects | `IS_INTERNAL = 0 AND NUM_METAREFRESH > 0` |
| External Client Error (4xx) | External URLs with 4xx responses | `IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 400 AND 499` |
| External Server Error (5xx) | External URLs with 5xx responses | `IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 500 AND 599` |

---

### Page Titles Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All page titles | (no filter) |
| Missing | Missing title tag | `TITLE_1 IS NULL OR TRIM(TITLE_1) = ''` |
| Duplicate | Duplicate title tag text | JOIN `APP.DUPLICATES_TITLE` |
| Over X Characters | Title length over 60 characters | `LENGTH(TITLE_1) > 60` |
| Below X Characters | Title length below 30 characters | `LENGTH(TITLE_1) < 30` |
| Over X Pixels | Title pixel width over threshold | TODO |
| Below X Pixels | Title pixel width below threshold | TODO |
| Same as H1 | Title text matches H1 | `TITLE_1 = H1_1` |
| Multiple | Multiple title tags on the page | `NUM_TITLES > 1` |
| Outside `<head>` | Title tag outside `<head>` | JOIN `APP.HTML_VALIDATION_DATA` |

---

### Meta Description Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All meta descriptions | (no filter) |
| Missing | Missing meta description tag | Complex meta name/content check |
| Duplicate | Duplicate meta description text | JOIN `APP.DUPLICATES_META_DESCRIPTION` |
| Over X Characters | Meta description over 155 characters | `LENGTH(desc_value) > 155` |
| Below X Characters | Meta description below 70 characters | `LENGTH(desc_value) < 70` |
| Over X Pixels | Meta description over pixel threshold | TODO |
| Below X Pixels | Meta description below pixel threshold | TODO |
| Multiple | Multiple meta description tags | Count of description meta tags > 1 |
| Outside `<head>` | Meta description outside `<head>` | JOIN `APP.HTML_VALIDATION_DATA` |

---

### Meta Keywords Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All meta keywords | (no filter) |
| Missing | Missing meta keywords tag | Complex meta name check |
| Duplicate | Duplicate meta keywords text | Subquery for duplicates |
| Multiple | Multiple meta keywords tags | Count of keyword meta tags > 1 |

---

### H1 Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All H1 entries | (no filter) |
| Missing | Missing H1 | `H1_1 IS NULL OR TRIM(H1_1) = ''` |
| Duplicate | Duplicate H1 text | JOIN `APP.DUPLICATES_H1` |
| Over X Characters | H1 length over 70 characters | `LENGTH(H1_1) > 70` |
| Multiple | Multiple H1 tags | `NUM_H1 > 1` |
| Alt Text in H1 | H1 sourced from image alt text | `H1_SOURCE_1 = 'IMG_ALT' OR ...` |
| Non-Sequential | Non-sequential heading order | `NON_SEQUENTIAL_H1 = 1` |

---

### H2 Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All H2 entries | (no filter) |
| Missing | Missing H2 | `H2_1 IS NULL OR TRIM(H2_1) = ''` |
| Duplicate | Duplicate H2 text | JOIN `APP.DUPLICATES_H2` |
| Over X Characters | H2 length over 70 characters | `LENGTH(H2_1) > 70` |
| Multiple | Multiple H2 tags | `NUM_H2 > 1` |
| Non-Sequential | Non-sequential heading order | `NON_SEQUENTIAL_H2 = 1` |

---

### Canonicals Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All canonicals | (no filter) |
| Canonicalised | URLs with canonicalised flag | `IS_CANONICALISED = 1` |
| Missing | Missing canonical tag | NOT EXISTS link type 6 |
| Multiple | Multiple canonical tags | COUNT link type 6 > 1 |
| Multiple Conflicting | Multiple conflicting canonicals | COUNT DISTINCT destinations > 1 |
| Canonical Is Relative | Canonical URL is relative | TODO |
| Contains Canonical | Contains canonical tag | EXISTS link type 6 |
| Contains Fragment URL | Canonical contains fragment URL | JOIN `APP.HTML_VALIDATION_DATA` |
| Invalid Attribute In Annotation | Canonical has invalid attribute | JOIN `APP.HTML_VALIDATION_DATA` |
| Outside `<head>` | Canonical outside `<head>` | JOIN `APP.HTML_VALIDATION_DATA` |
| Self Referencing | Self-referencing canonical | Src = Dst via link type 6 |
| Non-Indexable Canonical | Non-indexable canonical target | TODO |
| Unlinked | Canonicals with unlinked targets | JOIN `APP.MULTIMAP_CANONICALS_PENDING_LINK` |

---

### Directives Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All directive entries | (no filter) |
| Index | Index directive | Complex meta robots/X-Robots-Tag check |
| Noindex | Noindex directive | `LIKE '%noindex%'` in meta/X-Robots |
| Follow | Follow directive | Complex meta robots/X-Robots-Tag check |
| Nofollow | Nofollow directive | `LIKE '%nofollow%'` in meta/X-Robots |
| None | None directive | `LIKE '%none%'` in meta/X-Robots |
| NoArchive | Noarchive directive | `LIKE '%noarchive%'` in meta/X-Robots |
| NoSnippet | Nosnippet directive | `LIKE '%nosnippet%'` in meta/X-Robots |
| Max-Snippet | Max-snippet directive | `LIKE '%max-snippet%'` in meta/X-Robots |
| Max-Image-Preview | Max-image-preview directive | `LIKE '%max-image-preview%'` in meta/X-Robots |
| Max-Video-Preview | Max-video-preview directive | `LIKE '%max-video-preview%'` in meta/X-Robots |
| NoODP | NoODP directive | `LIKE '%noodp%'` in meta/X-Robots |
| NoYDIR | NoYDIR directive | `LIKE '%noydir%'` in meta/X-Robots |
| NoImageIndex | Noimageindex directive | `LIKE '%noimageindex%'` in meta/X-Robots |
| NoTranslate | Notranslate directive | `LIKE '%notranslate%'` in meta/X-Robots |
| Unavailable_After | Unavailable_after directive | `LIKE '%unavailable_after%'` in meta/X-Robots |
| Refresh | Meta refresh directive | `NUM_METAREFRESH > 0` |
| Outside `<head>` | Meta robots outside `<head>` | JOIN `APP.HTML_VALIDATION_DATA` |

---

### Images Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All image URLs | `CONTENT_TYPE LIKE 'image/%'` |
| Over X KB | Images over 100KB | `PAGE_SIZE > 102400` |
| Missing Alt Text | Missing alt text | JOIN `APP.MISSING_ALT_TEXT_TRACKER` |
| Missing Alt Attribute | Missing alt attribute | JOIN `APP.MISSING_ALT_ATTRIBUTE_TRACKER` |
| Alt Text Over X Characters | Alt text over threshold | JOIN `APP.ALT_TEXT_OVER_X_CHARACTERS_TRACKER` |
| Background Images | Background images | TODO |
| Incorrectly Sized Images | Incorrectly sized images | TODO |
| Missing Size Attributes | Missing width/height attributes | JOIN `APP.MISSING_SIZE_ATTRIBUTES` |

---

### Hreflang Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All hreflang entries | (no filter) |
| Contains hreflang | Contains hreflang annotations | EXISTS link type 13 |
| Non-200 hreflang URLs | Hreflang URLs with non-200 responses | JOIN `APP.MULTIMAP_HREF_LANG_NON_200_LINK` |
| Unlinked hreflang URLs | Unlinked hreflang URLs | TODO |
| Missing Return Links | Missing return links | JOIN `APP.MULTIMAP_HREF_LANG_MISSING_CONFIRMATION` |
| Inconsistent Language & Region Return Links | Inconsistent language/region | JOIN `APP.MULTIMAP_HREF_LANG_INCONSISTENT_LANGUAGE_CONFIRMATION` |
| Non-Canonical Return Links | Non-canonical return links | JOIN `APP.MULTIMAP_HREF_LANG_CANONICAL_CONFIRMATION` |
| Noindex Return Links | Noindex return links | JOIN `APP.MULTIMAP_HREF_LANG_NO_INDEX_CONFIRMATION` |
| Incorrect Language & Region Codes | Incorrect language/region codes | TODO |
| Multiple Entries | Multiple hreflang entries | COUNT link type 13 > 1 |
| Missing Self Reference | Missing self-referencing hreflang | NOT EXISTS self-ref link type 13 |
| Not Using Canonical | Not using canonical for hreflang | TODO |
| Missing X-Default | Missing x-default hreflang | `LOWER(HREF_LANG) = 'x-default'` |
| Missing | Missing hreflang | NOT EXISTS link type 13 |
| Outside `<head>` | Hreflang outside `<head>` | JOIN `APP.HTML_VALIDATION_DATA` |

---

### Pagination Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All pagination entries | (no filter) |
| Contains Pagination | Contains rel=next/prev | EXISTS link type 8 or 10 |
| First Page | First page (has next, no prev) | EXISTS next AND NOT EXISTS prev |
| Paginated 2+ Pages | Pagination with 2+ pages | EXISTS link type 8 or 10 |
| Pagination URL Not in Anchor Tag | Pagination URL not in anchor | JOIN `APP.MULTIMAP_PAGINATION_PENDING_LINK` |
| Non-200 Pagination URLs | Pagination URLs with non-200 | TODO |
| Unlinked Pagination URLs | Unlinked pagination URLs | TODO |
| Non-Indexable | Non-indexable pagination URLs | TODO |
| Multiple Pagination URLs | Multiple pagination URLs | COUNT > 1 |
| Pagination Loop | Pagination loop detected | Self-ref in link type 8/10 |
| Sequence Error | Pagination sequence error | JOIN `APP.MULTIMAP_PAGINATION_SEQUENCE_ERROR` |

---

### Structured Data Tab

| Filter | Description | Derby SQL |
|--------|-------------|-----------|
| All | All structured data entries | (no filter) |
| Contains Structured Data | URLs with structured data | `SERIALISED_STRUCTURED_DATA IS NOT NULL` |
| Missing | Missing structured data | `SERIALISED_STRUCTURED_DATA IS NULL` |
| Validation Errors | Structured data validation errors | TODO |
| Validation Warnings | Structured data validation warnings | TODO |
| Rich Result Validation Errors | Rich result validation errors | JOIN `APP.URL_INSPECTION` |
| Rich Result Validation Warnings | Rich result validation warnings | JOIN `APP.URL_INSPECTION` |
| Parse Errors | Structured data parse errors | TODO |
| Microdata URLs | URLs with Microdata | TODO |
| JSON-LD URLs | URLs with JSON-LD | TODO |
| RDFa URLs | URLs with RDFa | TODO |
| Rich Result Feature Detected | Rich result feature detected | JOIN `APP.URL_INSPECTION` |

---

### Internal Tab

| Filter | Description | Columns |
|--------|-------------|---------|
| All | All internal URLs | - |
| HTML | HTML content type | Content Type |
| JavaScript | JavaScript content type | Content Type |
| CSS | CSS content type | Content Type |
| Images | Image content types | Content Type |
| Plugins | Plugin content types | Content Type |
| Media | Media content types | Content Type |
| Fonts | Font content types | Content Type |
| XML | XML content type | Content Type |
| PDF | PDF content type | Content Type |
| Other | Other content types | Content Type |
| Unknown | Unknown content types | Content Type |

---

## Tab Name Normalization

Tab names are normalized for flexible access:

| Input | Normalized To |
|-------|---------------|
| `"Page Titles"` | `page_titles_all.csv` |
| `"page_titles"` | `page_titles_all.csv` |
| `"page_titles.csv"` | `page_titles.csv` |
| `"page_titles_missing"` | `page_titles_missing.csv` |
| `"response codes"` | `response_codes_all.csv` |
| `"Response Codes"` | `response_codes_all.csv` |

---

## Filter Registry API

### Module Functions

```python
from screamingfrog.filters.registry import (
    list_tabs,        # List all tabs with filters
    list_filters,     # List filters for a tab
    get_filter,       # Get specific filter definition
    all_filters,      # Iterate all filter definitions
)

# List all tabs with registered filters
tabs = list_tabs()
print(tabs)  # ['canonicals', 'directives', 'h1', 'h2', ...]

# List filters for a specific tab
filters = list_filters("Page Titles")
for filt in filters:
    print(f"{filt.name}: {filt.description}")

# Get a specific filter definition
filt = get_filter("Page Titles", "Missing")
print(filt.sql_where)  # "TITLE_1 IS NULL OR TRIM(TITLE_1) = ''"

# Iterate all registered filters
for filt in all_filters():
    print(f"{filt.tab}:{filt.name}")
```

### FilterDef Structure

```python
from dataclasses import dataclass
from typing import List, Optional

@dataclass(frozen=True)
class FilterDef:
    name: str                       # Filter name (e.g., "Missing")
    tab: str                        # Tab name (e.g., "Page Titles")
    description: str                # Human-readable description
    sql_where: Optional[str]        # SQL WHERE clause for Derby
    join_table: Optional[str]       # JOIN table if needed
    join_on: Optional[str]          # JOIN condition
    join_type: str                  # "LEFT" or "INNER"
    columns: List[str]              # Relevant column names
```

---

## SQLite Backend Filter Support

The SQLite backend supports a limited set of filters:

| Tab | Filter | Supported |
|-----|--------|-----------|
| Page Titles | Missing | Yes |
| Meta Description | Missing | Yes |
| Response Codes | Success (2xx) | Yes |
| Response Codes | Client Error (4xx) | Yes |
| Response Codes | Server Error (5xx) | Yes |

Other filters require the Derby or Hybrid backend.

---

## Common Tab Patterns

### Find All Missing Titles

```python
for page in crawl.tab("page_titles").filter(gui="Missing"):
    print(f"Missing title: {page['Address']}")
```

### Find Duplicate Descriptions

```python
for page in crawl.tab("meta_description").filter(gui="Duplicate"):
    print(f"Duplicate: {page['Address']} - {page.get('Meta Description 1')}")
```

### Find All 4xx Errors

```python
for page in crawl.tab("response_codes").filter(gui="Client Error (4xx)"):
    print(f"4xx: {page['Address']} - {page['Status Code']}")
```

### Find Pages with Noindex

```python
for page in crawl.tab("directives").filter(gui="Noindex"):
    print(f"Noindex: {page['Address']}")
```

### Find Orphan Pages (No Inlinks)

```python
for page in crawl.internal.filter(content_type="text/html"):
    inlinks = list(crawl.inlinks(page.address))
    if len(inlinks) == 0:
        print(f"Orphan: {page.address}")
```

---

## Performance Considerations

### CSV Backend
- Full file scan for each filter
- Slow for large crawls
- Filter availability depends on exported files

### Derby Backend
- Fast SQL queries with indexes
- JOINs for duplicate/validation filters
- Best performance for large crawls

### Hybrid Backend
- Derby speed for supported filters
- CSV fallback adds export overhead
- Cached exports reused on subsequent calls

---

## Limitations

- Some GUI filters are marked as TODO (not yet implemented)
- Pixel-based filters require client-side rendering (not available)
- Some filters depend on auxiliary tables that may be empty
- SQLite backend has limited filter support
- CSV backend filter availability depends on exported files
