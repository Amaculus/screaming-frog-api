# Escape Hatches (Raw Database Access)

This document covers raw table access, SQL passthrough, and advanced database queries.

---

## Overview

While the library provides high-level APIs for common tasks, sometimes you need direct database access for:

- Custom aggregations not exposed by the API
- Complex JOINs across multiple tables
- Debugging or exploring the database schema
- Performance optimization with custom queries

The escape hatches provide this access while maintaining backend abstraction.

---

## Backend Support

| Method | Derby | SQLite | CSV | CLI |
|--------|-------|--------|-----|-----|
| `crawl.raw(table)` | Yes | Yes | No | No |
| `crawl.sql(query)` | Yes | Yes | No | No |

CSV and CLI backends raise `NotImplementedError` for raw/SQL access.

---

## Raw Table Access

### `crawl.raw(table)` - Raw Table Iteration

Iterate all rows from a database table as dictionaries:

```python
# Access the main URLs table (Derby)
for row in crawl.raw("APP.URLS"):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"])

# Access links table
for row in crawl.raw("APP.LINKS"):
    print(row["SRC_ID"], row["DST_ID"], row["LINK_TYPE"])

# Access unique URLs
for row in crawl.raw("APP.UNIQUE_URLS"):
    print(row["ID"], row["ENCODED_URL"])
```

### Derby Tables

| Table | Description |
|-------|-------------|
| `APP.URLS` | Main URL data with all crawl fields |
| `APP.LINKS` | Link relationships (src, dst, type, anchor) |
| `APP.UNIQUE_URLS` | URL deduplication mapping |
| `APP.HTML_VALIDATION_DATA` | HTML validation flags |
| `APP.DUPLICATES_TITLE` | Duplicate title detection |
| `APP.DUPLICATES_META_DESCRIPTION` | Duplicate meta description detection |
| `APP.DUPLICATES_H1` | Duplicate H1 detection |
| `APP.DUPLICATES_H2` | Duplicate H2 detection |
| `APP.URL_INSPECTION` | Google URL Inspection data |
| `APP.MULTIMAP_*` | Various multimap tracking tables |

### SQLite Tables

SQLite schema varies by Screaming Frog version. Common patterns:

```python
# List tables in SQLite
for row in crawl.sql("SELECT name FROM sqlite_master WHERE type='table'"):
    print(row["name"])
```

---

## SQL Passthrough

### `crawl.sql(query, params)` - SQL Query Execution

Execute arbitrary SQL queries:

```python
# Simple query
for row in crawl.sql("SELECT ENCODED_URL, RESPONSE_CODE FROM APP.URLS"):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"])

# With parameters (parameterized queries)
for row in crawl.sql(
    "SELECT * FROM APP.URLS WHERE RESPONSE_CODE >= ?",
    [400]
):
    print(row["ENCODED_URL"], row["RESPONSE_CODE"])

# Multiple parameters
for row in crawl.sql(
    "SELECT * FROM APP.URLS WHERE RESPONSE_CODE BETWEEN ? AND ?",
    [400, 499]
):
    print(row["ENCODED_URL"])
```

---

## Common SQL Patterns

### Aggregations

```python
# Count by status code
for row in crawl.sql("""
    SELECT RESPONSE_CODE, COUNT(*) as cnt
    FROM APP.URLS
    GROUP BY RESPONSE_CODE
    ORDER BY cnt DESC
"""):
    print(f"{row['RESPONSE_CODE']}: {row['cnt']}")

# Average word count
for row in crawl.sql("""
    SELECT AVG(WORD_COUNT) as avg_words
    FROM APP.URLS
    WHERE WORD_COUNT IS NOT NULL
"""):
    print(f"Average word count: {row['avg_words']}")

# Content type distribution
for row in crawl.sql("""
    SELECT CONTENT_TYPE, COUNT(*) as cnt
    FROM APP.URLS
    WHERE IS_INTERNAL = 1
    GROUP BY CONTENT_TYPE
    ORDER BY cnt DESC
"""):
    print(f"{row['CONTENT_TYPE']}: {row['cnt']}")
```

### JOINs

```python
# Links with URL details
for row in crawl.sql("""
    SELECT
        src.ENCODED_URL as source_url,
        dst.ENCODED_URL as dest_url,
        l.ANCHOR,
        l.LINK_TYPE
    FROM APP.LINKS l
    JOIN APP.UNIQUE_URLS src ON l.SRC_ID = src.ID
    JOIN APP.UNIQUE_URLS dst ON l.DST_ID = dst.ID
    FETCH FIRST 100 ROWS ONLY
"""):
    print(f"{row['source_url']} -> {row['dest_url']}")

# Duplicate titles with page details
for row in crawl.sql("""
    SELECT u.ENCODED_URL, u.TITLE_1, d.DUPLICATE_KEY
    FROM APP.URLS u
    INNER JOIN APP.DUPLICATES_TITLE d ON u.ENCODED_URL = d.ENCODED_URL
    ORDER BY d.DUPLICATE_KEY
"""):
    print(f"{row['DUPLICATE_KEY']}: {row['ENCODED_URL']} - {row['TITLE_1']}")
```

### Subqueries

```python
# Pages with more than 10 inlinks
for row in crawl.sql("""
    SELECT u.ENCODED_URL, inlink_count
    FROM APP.URLS u
    JOIN (
        SELECT d.ENCODED_URL, COUNT(*) as inlink_count
        FROM APP.LINKS l
        JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID
        GROUP BY d.ENCODED_URL
        HAVING COUNT(*) > 10
    ) counts ON u.ENCODED_URL = counts.ENCODED_URL
    ORDER BY inlink_count DESC
"""):
    print(f"{row['inlink_count']}: {row['ENCODED_URL']}")

# Orphan pages (no inlinks)
for row in crawl.sql("""
    SELECT u.ENCODED_URL
    FROM APP.URLS u
    JOIN APP.UNIQUE_URLS uu ON u.ENCODED_URL = uu.ENCODED_URL
    WHERE u.IS_INTERNAL = 1
    AND uu.ID NOT IN (SELECT DISTINCT DST_ID FROM APP.LINKS)
"""):
    print(f"Orphan: {row['ENCODED_URL']}")
```

### Window Functions

```python
# Rank pages by word count
for row in crawl.sql("""
    SELECT
        ENCODED_URL,
        WORD_COUNT,
        RANK() OVER (ORDER BY WORD_COUNT DESC) as word_rank
    FROM APP.URLS
    WHERE WORD_COUNT IS NOT NULL AND IS_INTERNAL = 1
    FETCH FIRST 20 ROWS ONLY
"""):
    print(f"{row['word_rank']}. {row['WORD_COUNT']} words: {row['ENCODED_URL']}")
```

---

## Derby-Specific Queries

### Limit Results (Derby Syntax)

Derby uses `FETCH FIRST n ROWS ONLY` instead of `LIMIT`:

```python
# Derby: FETCH FIRST
for row in crawl.sql("""
    SELECT * FROM APP.URLS
    FETCH FIRST 10 ROWS ONLY
"""):
    print(row["ENCODED_URL"])

# Not supported in Derby:
# SELECT * FROM APP.URLS LIMIT 10
```

### String Functions

```python
# Case-insensitive search
for row in crawl.sql("""
    SELECT ENCODED_URL, TITLE_1
    FROM APP.URLS
    WHERE LOWER(TITLE_1) LIKE '%keyword%'
"""):
    print(row["TITLE_1"])

# String length
for row in crawl.sql("""
    SELECT ENCODED_URL, LENGTH(TITLE_1) as title_len
    FROM APP.URLS
    WHERE LENGTH(TITLE_1) > 60
    ORDER BY title_len DESC
    FETCH FIRST 20 ROWS ONLY
"""):
    print(f"{row['title_len']}: {row['ENCODED_URL']}")
```

### NULL Handling

```python
# Find NULL values
for row in crawl.sql("""
    SELECT ENCODED_URL
    FROM APP.URLS
    WHERE TITLE_1 IS NULL OR TRIM(TITLE_1) = ''
"""):
    print(f"Missing title: {row['ENCODED_URL']}")

# COALESCE for defaults
for row in crawl.sql("""
    SELECT ENCODED_URL, COALESCE(TITLE_1, '(no title)') as title
    FROM APP.URLS
    FETCH FIRST 10 ROWS ONLY
"""):
    print(f"{row['title']}: {row['ENCODED_URL']}")
```

---

## SQLite-Specific Queries

### Limit Results (SQLite Syntax)

```python
# SQLite: LIMIT
for row in crawl.sql("SELECT * FROM urls LIMIT 10"):
    print(row["address"])
```

### Schema Discovery

```python
# List all tables
for row in crawl.sql("SELECT name FROM sqlite_master WHERE type='table'"):
    print(row["name"])

# Table schema
for row in crawl.sql("PRAGMA table_info(urls)"):
    print(f"{row['name']}: {row['type']}")
```

---

## Advanced Examples

### Link Analysis

```python
# Find pages linking to 404s
for row in crawl.sql("""
    SELECT
        src.ENCODED_URL as source,
        dst.ENCODED_URL as broken_link,
        l.ANCHOR,
        u.RESPONSE_CODE
    FROM APP.LINKS l
    JOIN APP.UNIQUE_URLS src ON l.SRC_ID = src.ID
    JOIN APP.UNIQUE_URLS dst ON l.DST_ID = dst.ID
    JOIN APP.URLS u ON dst.ENCODED_URL = u.ENCODED_URL
    WHERE u.RESPONSE_CODE = 404
"""):
    print(f"{row['source']} -> {row['broken_link']} ({row['ANCHOR']})")

# Internal link counts per page
for row in crawl.sql("""
    SELECT
        src.ENCODED_URL,
        COUNT(*) as outlink_count,
        SUM(CASE WHEN u.IS_INTERNAL = 1 THEN 1 ELSE 0 END) as internal_links
    FROM APP.LINKS l
    JOIN APP.UNIQUE_URLS src ON l.SRC_ID = src.ID
    JOIN APP.UNIQUE_URLS dst ON l.DST_ID = dst.ID
    LEFT JOIN APP.URLS u ON dst.ENCODED_URL = u.ENCODED_URL
    WHERE l.LINK_TYPE = 1
    GROUP BY src.ENCODED_URL
    ORDER BY outlink_count DESC
    FETCH FIRST 20 ROWS ONLY
"""):
    print(f"{row['outlink_count']} ({row['internal_links']} internal): {row['ENCODED_URL']}")
```

### Content Analysis

```python
# Word count distribution
for row in crawl.sql("""
    SELECT
        CASE
            WHEN WORD_COUNT < 100 THEN 'thin (<100)'
            WHEN WORD_COUNT < 500 THEN 'short (100-500)'
            WHEN WORD_COUNT < 1000 THEN 'medium (500-1000)'
            WHEN WORD_COUNT < 2000 THEN 'long (1000-2000)'
            ELSE 'very long (2000+)'
        END as category,
        COUNT(*) as page_count
    FROM APP.URLS
    WHERE IS_INTERNAL = 1 AND CONTENT_TYPE = 'text/html'
    GROUP BY category
    ORDER BY MIN(WORD_COUNT)
"""):
    print(f"{row['category']}: {row['page_count']}")

# Pages with similar word counts
for row in crawl.sql("""
    SELECT
        FLOOR(WORD_COUNT / 100) * 100 as word_bucket,
        COUNT(*) as pages
    FROM APP.URLS
    WHERE WORD_COUNT IS NOT NULL
    GROUP BY word_bucket
    ORDER BY word_bucket
"""):
    print(f"{row['word_bucket']}-{row['word_bucket']+99}: {row['pages']} pages")
```

### Indexability Analysis

```python
# Non-indexable reasons
for row in crawl.sql("""
    SELECT
        INDEXABILITY_STATUS,
        COUNT(*) as cnt
    FROM APP.URLS
    WHERE IS_INTERNAL = 1 AND INDEXABILITY = 'Non-Indexable'
    GROUP BY INDEXABILITY_STATUS
    ORDER BY cnt DESC
"""):
    print(f"{row['INDEXABILITY_STATUS']}: {row['cnt']}")

# Indexable vs non-indexable by content type
for row in crawl.sql("""
    SELECT
        CONTENT_TYPE,
        SUM(CASE WHEN INDEXABILITY = 'Indexable' THEN 1 ELSE 0 END) as indexable,
        SUM(CASE WHEN INDEXABILITY = 'Non-Indexable' THEN 1 ELSE 0 END) as non_indexable
    FROM APP.URLS
    WHERE IS_INTERNAL = 1
    GROUP BY CONTENT_TYPE
    ORDER BY indexable DESC
"""):
    print(f"{row['CONTENT_TYPE']}: {row['indexable']} indexable, {row['non_indexable']} non-indexable")
```

---

## Error Handling

```python
try:
    results = list(crawl.sql("SELECT * FROM nonexistent_table"))
except Exception as e:
    print(f"SQL error: {e}")

# Check if raw access is supported
try:
    for row in crawl.raw("APP.URLS"):
        break  # Just test access
except NotImplementedError:
    print("Raw access not supported for this backend")
```

---

## Performance Tips

### Use Appropriate Indexes

Derby has indexes on common columns. Use them:

```python
# Good: Uses ENCODED_URL index
crawl.sql("SELECT * FROM APP.URLS WHERE ENCODED_URL = ?", [url])

# Less efficient: Full table scan
crawl.sql("SELECT * FROM APP.URLS WHERE TITLE_1 LIKE '%keyword%'")
```

### Limit Results Early

```python
# Good: Limit in SQL
for row in crawl.sql("SELECT * FROM APP.URLS FETCH FIRST 100 ROWS ONLY"):
    process(row)

# Less efficient: Fetch all, slice in Python
all_rows = list(crawl.sql("SELECT * FROM APP.URLS"))
for row in all_rows[:100]:
    process(row)
```

### Use Aggregations in SQL

```python
# Good: Aggregate in database
result = next(crawl.sql("SELECT COUNT(*) as cnt FROM APP.URLS"))
count = result["cnt"]

# Less efficient: Count in Python
count = sum(1 for _ in crawl.raw("APP.URLS"))
```

### Avoid SELECT *

```python
# Good: Select only needed columns
crawl.sql("SELECT ENCODED_URL, RESPONSE_CODE FROM APP.URLS")

# Less efficient: Select all columns
crawl.sql("SELECT * FROM APP.URLS")
```

---

## Mapping Raw Columns to CSV Names

Derby column names differ from CSV export headers. The mapping is defined in `schemas/mapping.json`:

| Derby Column | CSV Header |
|--------------|------------|
| `ENCODED_URL` | `Address` |
| `RESPONSE_CODE` | `Status Code` |
| `CONTENT_TYPE` | `Content Type` |
| `PAGE_SIZE` | `Size` |
| `WORD_COUNT` | `Word Count` |
| `TITLE_1` | `Title 1` |
| `H1_1` | `H1-1` |
| `IS_INTERNAL` | `(internal flag)` |
| `INDEXABILITY` | `Indexability` |

---

## Limitations

- **Backend-specific**: Only Derby and SQLite backends support raw/SQL access
- **Column names**: Derby uses internal column names, not CSV headers
- **Syntax differences**: Derby and SQLite have different SQL dialects
- **No transactions**: Each query runs independently
- **Read-only**: Cannot modify the database (by design)
- **Memory**: Large result sets load into memory; use LIMIT/FETCH
