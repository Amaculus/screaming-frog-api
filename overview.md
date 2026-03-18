# Screaming Frog Python Library - Progress Overview

Last updated: 2026-03-17

## Goals
- Single Python API for all Screaming Frog crawl sources:
  - `.seospider` (CLI load to DB mode)
  - `.dbseospider` (Derby)
  - Internal DB crawl IDs / ProjectInstanceData
  - CSV export folders
- Access all GUI tabs (as exported CSVs) and keep docs current.

## What's done
### 1) Crawl source support
- **CSV export folders**: `Crawl.load("./exports")`
- **SQLite**: `Crawl.load("./crawl.db")` (internal tab only)
- **Derby `.dbseospider`**: direct read via `DerbyBackend` and mapping
- **Internal DB crawl directories**: load Derby root directly
- **`.seospider`**: CLI load -> DB mode -> Derby backend (default), auto-materializes `.dbseospider`
- **DB crawl IDs**: Derby backend (default)

### 2) CLI export + kitchen-sink profile
- CLI export helper (`screamingfrog/cli/exports.py`)
- Default export: `Internal:All`
- **Kitchen-sink export profile** bundled:
  - `screamingfrog/config/exports_kitchen_sink_tabs.txt`
  - `screamingfrog/config/exports_kitchen_sink_bulk.txt`
  - `Crawl.load(..., export_profile="kitchen_sink")`

### 3) Generic tab API
- Any CSV export is available via `crawl.tab("response_codes_all")`
- `crawl.tabs` lists available CSVs
- Works for CSV exports and Derby (mapping-backed)
- Derby inlinks/outlinks available via `crawl.inlinks(url)` / `crawl.outlinks(url)`
- Raw escape hatches for DB backends: `crawl.raw("APP.URLS")` and `crawl.sql("SELECT ...")`
- Chainable query builder for DB backends: `crawl.query(...).select(...).where(...).collect()`
- Crawl-over-crawl diff available via `crawl.compare(other_crawl)` (status, title, redirect, and selected content/indexability signals)
- Derby-backed `crawl.internal` now materializes expression-backed fields like `Indexability` and `Indexability Status`
- Derby filters now support mapped expressions plus post-filter header-derived fields in `crawl.internal` and generic tabs
- Derby special tabs added for cookies, spelling/grammar, and structured-data summary/detail exports
- SQLite `crawl.internal` now follows the `internal_all` projection instead of bypassing mapped fields

### 4) Derby support without SF install
- Bundled Apache Derby jars (10.17.1.0) under `screamingfrog/vendor/derby`
- `DERBY_JAR` is optional override

### 5) `.dbseospider` packaging
- `pack_dbseospider(project_dir, output_file)`
- `pack_dbseospider_from_db_id(db_id, output_file)`
- `export_dbseospider_from_seospider(crawl_path, output_file)`
- `load_seospider_db_project(crawl_path, ...)`
- `unpack_dbseospider(dbseospider_file, output_dir)`

### 6) Schema discovery
- Full kitchen-sink CSV export set generated (628 CSVs)
- CSV schemas written to `schemas/csv`
- Derby schema generated from ActionNetwork and Nitafc `.dbseospider`
- `schemas/mapping.json` used for Derby tab mapping
- Mapping rebuilt from schema + augmented for core tabs (response codes, titles, headings, directives, images, pagination, hreflang, canonicals, pagespeed, accessibility).
- Derby mapping supports `db_expression` for computed columns (indexability, lengths, meta description/keywords, meta robots, dimensions, inlink counts, canonical counts).
- HTTP canonical/rel next/prev parsing wired via header blob extraction.
- Remaining unmapped CSV columns: pixel width, some structured data validation errors/warnings, link score/% of total, JS-specific outlink counts, accessibility status, secondary hreflang slots.

### 7) Config builder + crash fix
- Java ConfigBuilder updated to avoid custom extraction crash
- Python `sf-config-builder` bundled updated JAR + version bumps

## Known limitations
- SQLite backend supports only a small set of high-value tabs; full GUI parity is not implemented.
- Derby tab mapping depends on `schemas/mapping.json`.
- GUI "tabs" are filtered views; only CSV exports guarantee identical filters.
- .seospider conversion requires SF CLI and DB storage mode; helper can force storage.mode=DB.
- .seospider auto-materialization overwrites `.dbseospider` by default; set `dbseospider_overwrite=False` to reuse.
- Response Codes filters populated with SQL WHERE clauses (JS redirect + loop still pending; internal redirect chain implemented).
- Page Titles filters use duplicates table + length checks; pixel-width still pending.
- Meta Description filters use meta-name/content expressions + duplicates table; pixel-width still pending.
- H1/H2 filters use duplicates tables; H1 alt-text source now mapped; H2 alt-text still N/A.
- Directives filters now check meta-robots and X-Robots-Tag content; outside-head flag works.
- Images filters use tracker tables for missing alt/size; background/incorrect sizing still pending.
- Pagination filters partially implemented via multimap tables (pending link + sequence error).
- Structured Data filters include rich-result checks via URL Inspection; validation errors/warnings still pending.
- Canonicals now map canonical link element via link-type subquery; filters for contains/missing/multiple/self-referencing now use link-type joins.
- Hreflang columns now use link-type subqueries + sitemap results for HTML/sitemap fields; HTTP hreflang mapped via LINK_TYPE=12 where present; filters for contains/missing/multiple/self/x-default and not-using-canonical added.
- Pagination rel next/prev mapped via link types (LINK_TYPE=10 next, LINK_TYPE=8 prev) where present.
- HTTP Canonical + HTTP rel next/prev now parsed from `HTTP_RESPONSE_HEADER_COLLECTION` (gzipped JSON blob).
- GUI filter execution wired into tab queries (CSV uses filtered file; Derby uses SQL WHERE + optional JOIN).
- All GUI tab filters auto-registered from the kitchen-sink export list.
- Indexability columns now mapped via heuristic (robots.txt + noindex in meta/X-Robots).
- Tab name normalization added (e.g., "Page Titles" -> `page_titles_all.csv`).
- Convenience helpers added: `tab_filters`, `tab_columns`, `describe_tab`.
- Raw/SQL escape hatches are only available for Derby/SQLite backends.

## Recommended usage
- For GUI-accurate tabs: **CLI export** with `export_profile="kitchen_sink"` and read CSVs.
- For fast queries: **Derby `.dbseospider`** (mapping-backed).

## Possible next steps
0) **Filter registry (in progress)**
   - Replace auto-generated filters with explicit SQL WHERE clauses per tab.
   - Start with Response Codes, Titles, Meta Description, H1/H2, Canonicals.
   - Wire `crawl.tab(...).filter(gui="Missing Title")` style filters.
1) **SQLite parity**
   - Auto-convert Derby -> SQLite and support generic tabs on SQLite.
2) **Typed models for more tabs**
   - Add strong models for response codes, inlinks/outlinks, canonicals, titles, metas, etc.
3) **Export convenience**
   - Helper to export kitchen-sink from `.dbseospider` via CLI (`--load-crawl` + profile).
4) **Mapping maintenance**
   - Auto-refresh `schemas/mapping.json` from new kitchen-sink exports.
5) **Tests**
   - Integration tests comparing CSV vs Derby for key tabs.
