# Limits and Gaps

This document covers current limitations, dependencies, and known gaps in the library.

---

## Overview

The library is designed to provide programmatic access to Screaming Frog crawl data. While it covers most common use cases, there are limitations inherent to the approach and the underlying data formats.

---

## Backend Limitations

### Feature Support by Backend

| Feature | Derby | Hybrid | CSV | SQLite | CLI |
|---------|-------|--------|-----|--------|-----|
| Internal pages | Yes | Yes | Yes | Yes | Yes |
| Tab filtering | Yes | Yes | Yes | Partial | Yes |
| GUI filters | Most | Most | Yes | Limited | Yes |
| Inlinks/Outlinks | Yes | Yes | No | No | No |
| Raw SQL | Yes | Yes | No | Yes | No |
| Crawl diff | Yes | Yes | Yes | Yes | Yes |
| Custom extractions | Yes | Yes | Yes | Yes | Yes |

### Derby Backend

**Strengths:**
- Full link graph access (inlinks/outlinks)
- Raw SQL queries
- No CLI dependency for queries
- Fast for complex queries

**Limitations:**
- Requires Java runtime
- Requires Derby JAR files
- Some columns mapped to NULL
- Schema varies by SF version

### CSV Backend

**Strengths:**
- No external dependencies
- Full GUI filter support
- Human-readable column names
- Works with exported data

**Limitations:**
- No link graph access
- No raw SQL queries
- Requires CLI for export
- Slower for large crawls

### SQLite Backend

**Strengths:**
- Native Python support
- SQL query capability
- No Java dependency

**Limitations:**
- Limited tab support
- No link graph access
- Schema varies by SF version
- Fewer high-level APIs

### CLI Backend

**Strengths:**
- Full SF feature support
- Accurate GUI parity
- Works with any crawl format

**Limitations:**
- Requires SF installation
- Requires license for some features
- Slower (spawns processes)
- No link graph access

### Hybrid Backend

**Strengths:**
- Best of both worlds
- Automatic fallback
- Full feature coverage

**Limitations:**
- Requires both Derby and CSV setup
- More complex configuration
- May have inconsistent behavior

---

## GUI Filter Support

### Fully Supported Filters

The following filter categories have full Derby implementation:

- **Response Codes**: All 30+ filters
- **Page Titles**: All 10 filters
- **Meta Description**: All 9 filters
- **H1/H2 Headings**: All 12 filters
- **Canonicals**: All 12 filters
- **Directives**: All 18 filters
- **Images**: All 8 filters
- **Hreflang**: All 15 filters
- **Pagination**: All 11 filters
- **Structured Data**: All 12 filters
- **Internal**: All 12 content type filters
- **Meta Keywords**: All 4 filters

### Partially Supported

Some filters require CSV fallback:

- **Accessibility**: Requires AXE_CORE_RESULTS table
- **PageSpeed**: Requires PAGE_SPEED_API table
- **Search Console**: Requires external API data

### Not Supported

- **Custom extraction filters**: Depends on crawl configuration
- **JavaScript-specific filters**: Requires JS rendering data
- **Security filters**: HTTPS, mixed content checks

---

## Column Mapping Gaps

### NULL-Mapped Columns

Some columns cannot be derived from Derby and return NULL:

```
Status                  # UI display status
Crawl Depth             # Computed during crawl
URL Path Folder Depth   # Computed path analysis
Hash                    # Content hash (not stored)
```

See `schemas/mapping_nulls.md` for the complete list.

### Missing Columns

Some CSV columns have no Derby equivalent:

- Computed metrics specific to SF algorithms
- API integration data (GSC, PSI)
- Real-time crawl statistics

---

## Data Format Limitations

### .seospider Files

**Issue**: Not directly readable

**Workaround**: Load via CLI to ProjectInstanceData, then access Derby

```python
# Requires SF CLI
crawl = Crawl.load("./crawl.seospider")
```

### .dbseospider Files

**Issue**: ZIP archive format

**Workaround**: Library handles unpacking automatically

```python
# Automatic unpacking
crawl = Crawl.load("./crawl.dbseospider")
```

### DB-Mode Projects

**Issue**: Live Derby database requires exclusive access

**Workaround**: Don't access while SF is running

```python
# Close SF first
crawl = Crawl.from_db_id("uuid-id-here")
```

---

## Derby-Specific Limitations

### SQL Dialect

Derby uses non-standard SQL syntax:

```sql
-- Derby: FETCH FIRST
SELECT * FROM APP.URLS FETCH FIRST 10 ROWS ONLY

-- Not supported: LIMIT
SELECT * FROM APP.URLS LIMIT 10  -- ERROR
```

### No Full-Text Search

Derby doesn't support full-text search:

```sql
-- Use LIKE instead
SELECT * FROM APP.URLS WHERE TITLE_1 LIKE '%keyword%'
```

### Limited Window Functions

Some window functions may not work:

```sql
-- Works
RANK() OVER (ORDER BY WORD_COUNT DESC)

-- May not work
LAG(), LEAD(), NTILE()
```

---

## Performance Limitations

### Large Crawls

For crawls with millions of URLs:

- Memory usage increases with result set size
- Complex SQL queries may be slow
- Link traversal can be memory-intensive

**Mitigations:**

```python
# Use LIMIT/FETCH
for row in crawl.sql("SELECT * FROM APP.URLS FETCH FIRST 1000 ROWS ONLY"):
    process(row)

# Iterate instead of collecting
for page in crawl.internal:  # Generator
    process(page)

# Avoid: Loading all into memory
all_pages = list(crawl.internal)  # Memory heavy
```

### Expression Performance

Complex CASE expressions are slower:

```python
# Indexability check scans 40+ meta tag columns
# This is slower than direct column access
```

### Link Graph Performance

Full link traversal can be slow:

```python
# Slow for pages with many links
inlinks = list(crawl.inlinks(url))  # May be 10,000+ links

# Better: Limit results
for link in crawl.inlinks(url):
    if count > 100:
        break
    process(link)
```

---

## Dependencies

### Required

| Dependency | Purpose | Installation |
|------------|---------|--------------|
| Python 3.9+ | Runtime | System |

### Optional

| Dependency | Purpose | Required For |
|------------|---------|--------------|
| Java 8+ | Derby access | Derby/Hybrid backend |
| Derby JAR | Database driver | Derby/Hybrid backend |
| SF CLI | Export/conversion | CLI backend, .seospider loading |
| SF License | Full features | Some CLI operations |

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `DERBY_JAR` | Path to Derby JAR | Bundled |
| `SCREAMINGFROG_CLI` | Path to SF CLI | Auto-detect |
| `SCREAMINGFROG_PROJECT_DIR` | ProjectInstanceData location | Platform default |
| `JAVA_HOME` | Java installation | Auto-detect |

---

## Platform Considerations

### Windows

- CLI paths include `(x86)` variants
- Path separators: Use raw strings `r"C:\path"`
- Long paths may need enabling

### macOS

- CLI in application bundle
- Java may need separate installation
- Permissions for app access

### Linux

- May need Xvfb for headless CLI
- Java package varies by distribution
- CLI may not be in PATH

---

## Compatibility

### SF Version Compatibility

The library is tested with recent SF versions. Older versions may have:

- Different Derby schema
- Different column names
- Missing tables

### Python Version Support

| Python | Status |
|--------|--------|
| 3.9 | Supported |
| 3.10 | Supported |
| 3.11 | Supported |
| 3.12 | Supported |
| 3.13 | Testing |

---

## Known Issues

### Concurrent Access

Don't access Derby while SF is running:

```python
# Bad: SF has lock on database
crawl = Crawl.from_db_id("uuid")  # May fail

# Good: Close SF first
```

### Encoding Issues

Some URLs may have encoding issues:

```python
# URL may be percent-encoded differently
url = page.address  # Check encoding
```

### Memory with Large Results

Collecting large result sets consumes memory:

```python
# May run out of memory
all_links = list(crawl.inlinks(url))  # 1M+ links

# Better: Stream processing
for link in crawl.inlinks(url):
    process(link)
```

---

## Workarounds

### Missing Filter Support

Use raw SQL for unsupported filters:

```python
# Custom filter via SQL
for row in crawl.sql("""
    SELECT ENCODED_URL FROM APP.URLS
    WHERE CUSTOM_CONDITION = 'value'
"""):
    print(row["ENCODED_URL"])
```

### Missing Column Data

Use CSV fallback for missing Derby columns:

```python
# Hybrid backend with CSV fallback
crawl = Crawl.load(
    "./crawl.dbseospider",
    backend="hybrid",
    csv_fallback=True,
)
```

### Performance Issues

Optimize queries for large datasets:

```python
# Use SQL aggregations instead of Python
result = next(crawl.sql("SELECT COUNT(*) as cnt FROM APP.URLS"))
count = result["cnt"]

# Instead of
count = sum(1 for _ in crawl.internal)  # Slower
```

---

## Future Improvements

### Planned Features

- Additional GUI filter support
- Better error messages
- Performance optimizations
- More backend options

### Contributions Welcome

- Additional Derby column mappings
- New filter implementations
- Documentation improvements
- Bug fixes and tests

---

## Reporting Issues

When reporting issues, include:

1. Python version
2. SF version
3. Backend being used
4. Minimal reproduction code
5. Error traceback
6. Sample data (if shareable)

```python
import screamingfrog
print(f"Library version: {screamingfrog.__version__}")
print(f"Python: {sys.version}")
```

