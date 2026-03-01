# Derby Schema Mapping

This document covers the mapping system that translates Derby database columns to CSV-compatible column names.

---

## Overview

Screaming Frog stores crawl data differently depending on the storage mode:

- **CSV exports**: Human-readable column headers (e.g., "Status Code", "Content Type")
- **Derby database**: Internal column names (e.g., "RESPONSE_CODE", "CONTENT_TYPE")

The mapping system bridges this gap, allowing the library to:

1. Query Derby tables with internal column names
2. Return data with CSV-compatible headers
3. Compute derived fields (indexability, link types) using SQL expressions

---

## Mapping File

### Location

```
schemas/mapping.json
```

### Structure

The mapping file is organized by CSV filename, with each entry containing column mappings:

```json
{
  "internal_all.csv": [
    {
      "csv_column": "Address",
      "db_column": "ENCODED_URL",
      "db_table": "APP.URLS"
    },
    {
      "csv_column": "Status Code",
      "db_column": "RESPONSE_CODE",
      "db_table": "APP.URLS"
    },
    {
      "csv_column": "Indexability",
      "db_expression": "CASE WHEN ... THEN 'Indexable' ELSE 'Non-Indexable' END",
      "db_table": "APP.URLS"
    }
  ]
}
```

---

## Mapping Types

### Direct Column Mapping

Simple one-to-one column mapping:

```json
{
  "csv_column": "Address",
  "db_column": "ENCODED_URL",
  "db_table": "APP.URLS"
}
```

### Expression Mapping

Computed values using SQL expressions:

```json
{
  "csv_column": "Link Type",
  "db_expression": "CASE WHEN APP.LINKS.LINK_TYPE = 1 THEN 'Hyperlink' WHEN APP.LINKS.LINK_TYPE = 6 THEN 'Canonical' ... END",
  "db_table": "APP.LINKS"
}
```

### Subquery Mapping

Values fetched from related tables:

```json
{
  "csv_column": "Source",
  "db_expression": "(SELECT s.ENCODED_URL FROM APP.UNIQUE_URLS s WHERE s.ID = APP.LINKS.SRC_ID FETCH FIRST 1 ROWS ONLY)",
  "db_table": "APP.LINKS"
}
```

### NULL Mapping

Columns that cannot be derived from Derby:

```json
{
  "csv_column": "Status",
  "db_expression": "NULL",
  "db_table": "APP.URLS"
}
```

---

## Coverage

### Total Mappings

The mapping file covers **628 CSV schema files**, including:

- All standard tabs (Internal, External, Response Codes, etc.)
- Filtered views (4xx, 5xx, Missing titles, etc.)
- Bulk exports (All Links, Sitemaps, etc.)
- Accessibility reports
- Structured data reports

### NULL-Mapped Columns

Some columns cannot be derived from Derby and are mapped to NULL:

- **Status column**: UI-specific display status
- **Certain computed metrics**: Some SF-proprietary calculations
- **API-specific fields**: Google Search Console, PageSpeed Insights data

See `schemas/mapping_nulls.md` for the complete list.

---

## Key Mappings

### Core URL Fields

| CSV Column | Derby Column | Table |
|------------|--------------|-------|
| Address | ENCODED_URL | APP.URLS |
| Status Code | RESPONSE_CODE | APP.URLS |
| Content Type | CONTENT_TYPE | APP.URLS |
| Size | PAGE_SIZE | APP.URLS |
| Word Count | WORD_COUNT | APP.URLS |
| Title 1 | TITLE_1 | APP.URLS |
| Meta Description 1 | META_DESCRIPTION_1 | APP.URLS |
| H1-1 | H1_1 | APP.URLS |
| H2-1 | H2_1 | APP.URLS |

### Computed Fields

| CSV Column | Expression Type | Description |
|------------|----------------|-------------|
| Indexability | CASE expression | Derives from robots.txt, meta robots, X-Robots-Tag |
| Indexability Status | CASE expression | Reason for non-indexability |
| Link Type | CASE expression | Maps LINK_TYPE IDs to labels |
| Internal/External | Boolean check | Based on IS_INTERNAL flag |

### Link Type Mappings

```sql
CASE
  WHEN LINK_TYPE = 1 THEN 'Hyperlink'
  WHEN LINK_TYPE = 6 THEN 'Canonical'
  WHEN LINK_TYPE = 8 THEN 'Rel Prev'
  WHEN LINK_TYPE = 10 THEN 'Rel Next'
  WHEN LINK_TYPE = 12 THEN 'Hreflang (HTTP)'
  WHEN LINK_TYPE = 13 THEN 'Hreflang'
  ELSE CAST(LINK_TYPE AS VARCHAR(10))
END
```

---

## Derby Tables

### Primary Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| APP.URLS | Main URL data | ENCODED_URL, RESPONSE_CODE, CONTENT_TYPE, etc. |
| APP.UNIQUE_URLS | URL deduplication | ID, ENCODED_URL |
| APP.LINKS | Link relationships | SRC_ID, DST_ID, LINK_TYPE, LINK_TEXT |

### Secondary Tables

| Table | Description |
|-------|-------------|
| APP.HTML_VALIDATION_DATA | HTML validation flags |
| APP.DUPLICATES_TITLE | Duplicate title detection |
| APP.DUPLICATES_META_DESCRIPTION | Duplicate meta description detection |
| APP.DUPLICATES_H1 | Duplicate H1 detection |
| APP.DUPLICATES_H2 | Duplicate H2 detection |
| APP.AXE_CORE_RESULTS | Accessibility audit results |
| APP.PAGE_SPEED_API | PageSpeed Insights data |
| APP.URL_INSPECTION | Google URL Inspection data |
| APP.MULTIMAP_* | Various multimap tracking tables |

---

## Indexability Expression

The indexability calculation is one of the most complex mappings:

```sql
CASE
  WHEN LOWER(CAST(BLOCKED_BY_ROBOTS_TXT AS VARCHAR(10))) IN ('1','true')
    THEN 'Non-Indexable'
  WHEN (
    -- Check all 20 meta robot tags for noindex
    (LOWER(META_NAME_1) IN ('robots', 'googlebot', ...) AND LOWER(META_CONTENT_1) LIKE '%noindex%')
    OR ...
    -- Check X-Robots-Tag headers
    OR LOWER(X_ROBOT_TAG_1) LIKE '%noindex%'
    OR ...
  )
    THEN 'Non-Indexable'
  ELSE 'Indexable'
END
```

This checks:
1. robots.txt blocking
2. Meta robots tags (up to 20)
3. JavaScript-injected meta tags (up to 20)
4. X-Robots-Tag headers (up to 20)

---

## Derby-Only Enrichments

Some data is richer in Derby than CSV exports:

### Link Type Labels

Derby stores link types as numeric IDs, which the library expands to human-readable labels:

| ID | Label |
|----|-------|
| 1 | Hyperlink |
| 6 | Canonical |
| 8 | Rel Prev |
| 10 | Rel Next |
| 12 | Hreflang (HTTP) |
| 13 | Hreflang |

### Inlinks/Outlinks

The library provides full link graph traversal from Derby that isn't available in standard CSV exports.

### Chain Reports

Redirect and canonical chain analysis is computed in Python from Derby data, providing richer details than SF's built-in chain reports.

---

## Fallback Behavior

### Hybrid Backend

The Hybrid backend checks mapping coverage before querying:

1. **Full coverage**: Use Derby for the query
2. **Missing columns**: Fall back to CSV export
3. **Missing GUI filters**: Fall back to CSV export

```python
# Hybrid automatically chooses the best backend
crawl = Crawl.load(
    "./crawl.dbseospider",
    backend="hybrid",
    csv_fallback=True,
)
```

### CSV Fallback Triggers

- Column not mapped in mapping.json
- GUI filter not implemented for Derby
- Complex aggregations not supported in Derby SQL

---

## HTTP Header Parsing

Some fields require parsing HTTP response headers from blob storage:

### Canonical from HTTP Header

```python
# Parsed from Link header: <https://example.com/page>; rel="canonical"
```

### X-Robots-Tag from HTTP Header

```python
# Parsed from X-Robots-Tag header values
```

### Rel Prev/Next from HTTP Header

```python
# Parsed from Link header with rel="prev" or rel="next"
```

---

## NULL Mapping Documentation

### schemas/mapping_nulls.md

Lists all columns mapped to NULL:

```markdown
# Mapping NULL Columns

Updated mappings: 628

Columns currently mapped to NULL in Derby:

- accessibility_accessibility_score_good.csv: Status
- accessibility_accessibility_score_needs_improvement.csv: Status
...
```

### schemas/inlinks_mapping_nulls.md

Lists NULL columns specific to inlinks tabs.

---

## Adding Custom Mappings

### Extending Mappings

To add support for new columns:

1. Identify the Derby column name
2. Add entry to mapping.json
3. Test with raw SQL queries first

```json
{
  "csv_column": "My Custom Field",
  "db_column": "CUSTOM_FIELD_NAME",
  "db_table": "APP.URLS"
}
```

### Computed Field Example

```json
{
  "csv_column": "Is Large Page",
  "db_expression": "CASE WHEN PAGE_SIZE > 1000000 THEN 'Yes' ELSE 'No' END",
  "db_table": "APP.URLS"
}
```

---

## Query Generation

### How Mappings Are Used

The library generates SQL queries from mappings:

```python
# Internal representation
mapping = {
    "csv_column": "Status Code",
    "db_column": "RESPONSE_CODE",
    "db_table": "APP.URLS"
}

# Generated SQL
# SELECT RESPONSE_CODE AS "Status Code" FROM APP.URLS
```

### Expression Handling

```python
# Expression mapping
mapping = {
    "csv_column": "Indexability",
    "db_expression": "CASE WHEN ... END",
    "db_table": "APP.URLS"
}

# Generated SQL
# SELECT (CASE WHEN ... END) AS "Indexability" FROM APP.URLS
```

---

## Performance Considerations

### Indexed Columns

Derby has indexes on frequently queried columns:

- ENCODED_URL (primary lookup)
- RESPONSE_CODE (status filtering)
- IS_INTERNAL (internal/external split)
- SRC_ID, DST_ID (link traversal)

### Expression Performance

Complex CASE expressions may be slower than direct column lookups:

```sql
-- Fast: direct column
SELECT RESPONSE_CODE FROM APP.URLS

-- Slower: complex expression
SELECT CASE WHEN ... (20+ conditions) ... END FROM APP.URLS
```

### Subquery Performance

Subqueries for related data add overhead:

```sql
-- Subquery for link source URL
(SELECT s.ENCODED_URL FROM APP.UNIQUE_URLS s WHERE s.ID = APP.LINKS.SRC_ID)
```

---

## Debugging Mappings

### Check Available Columns

```python
# List Derby tables
for row in crawl.sql("SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLETYPE='T'"):
    print(row["TABLENAME"])

# List columns in APP.URLS
for row in crawl.sql("""
    SELECT COLUMNNAME, COLUMNDATATYPE
    FROM SYS.SYSCOLUMNS c
    JOIN SYS.SYSTABLES t ON c.REFERENCEID = t.TABLEID
    WHERE t.TABLENAME = 'URLS'
"""):
    print(f"{row['COLUMNNAME']}: {row['COLUMNDATATYPE']}")
```

### Test Mapping Expression

```python
# Test a mapping expression
for row in crawl.sql("""
    SELECT ENCODED_URL,
           CASE WHEN RESPONSE_CODE >= 400 THEN 'Error' ELSE 'OK' END AS status
    FROM APP.URLS
    FETCH FIRST 10 ROWS ONLY
"""):
    print(f"{row['status']}: {row['ENCODED_URL']}")
```

---

## Limitations

- **Version differences**: Derby schema may vary by SF version
- **NULL columns**: Some data simply isn't stored in Derby
- **Expression limits**: Some SF calculations are proprietary
- **Performance**: Complex expressions slower than direct columns
- **Maintenance**: Mappings need updates when SF schema changes

