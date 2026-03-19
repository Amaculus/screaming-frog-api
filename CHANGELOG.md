# Changelog

## Unreleased
- Derby tab projection now skips literal `NULL` mapping columns at query time, which fixes oversized mapped tabs like `all_inlinks` / `all_outlinks` while still returning those fields as `None`.
- Derby `crawl.internal` now materializes expression-backed internal fields, including `Indexability` and `Indexability Status`.
- Derby filter compilation now supports mapped `db_expression` fields and post-filters mapped header/supplementary fields for `crawl.internal` and `crawl.tab(...)`.
- SQLite `crawl.internal` now follows the `internal_all` projection so mapped fields like `Indexability` / `Indexability Status` stay aligned with `crawl.tab("internal_all")`.
- Added a live smoke harness for multi-row custom extraction materialization behind `SCREAMINGFROG_RUN_LIVE_SMOKE=1`.
- Added Derby-backed special tab support for cookies, spelling/grammar, and structured-data summary/detail exports.
- Expanded mapping coverage and refreshed `schemas/mapping_nulls.md` / `schemas/inlinks_mapping_nulls.md`.
- Added direct Derby mappings for content language tabs, image alt-text length, and ten PageSpeed report savings columns.
- Added direct Derby mappings for `preload_key_requests_report`, `properly_size_images_report`, and mobile `PSI Request Status` fields.
- Added direct Derby mappings for content/internal readability metrics, near-duplicate fields, language/hash fields, and `mobile_all` PSI request status.
- Added direct Derby mappings for internal/url encoded-address, timestamp, response-time, and last-modified fields.
- Added derived Derby mappings for `Readability` across content and internal tabs using Screaming Frog's documented Flesch score groups.
- Added direct Derby mappings for URL-family `Length`, `ai_all` crawl timestamps, and form action link destination URLs.
- Added Derby mappings for JavaScript diff tabs (title, H1, meta description, robots, word-count deltas), lorem ipsum occurrences, and viewport content.
- Added Derby mappings for semantic similarity and low-relevance content fields via `APP.COSINE_SIMILARITY` and `APP.LOW_RELEVANCE`.
- Added direct Derby mappings for JS outlink counts, hreflang link labels in non-200/unlinked reports, and webfont-load PageSpeed savings.
- Added bulk Derby mappings for custom filter counts (`Filter 1`-`Filter 100`) and first-match custom extractor columns across internal/custom-extraction tabs plus `all_inlinks`.
- Added `multi_row_extract` mapping support in the Derby backend and used it to map the remaining custom-extractor match columns (`2..10`) across `custom_extraction_all`, `internal_all`, `internal_html`, and `all_inlinks`.
- Added exact current-state carryover mappings for `change_detection_*` tabs plus `Crawl Timestamp` where the value exists in a single Derby crawl.
- Added `pagespeed_main_thread_work` blob extraction support and mapped `minimize_main_thread_work_report.csv` from `APP.PAGE_SPEED_API.JSON_RESPONSE`.
- Added bulk Derby mappings for internal performance, CrUX, semantic-similarity, text-ratio, link-score, and transfer/CO2 fields across internal, links, and validation tabs.
- Added internal redirect/blocked-resource carryover mappings for `Redirect URL` / `Redirect Type`, aligned redirect labels to `HTTP Redirect`, and filled internal PageSpeed issue summary text plus pending-link `Unlinked` flags.
- Added backend-derived redirect URL materialization from meta-refresh targets and HTTP `Location` headers, plus generic blob-derived cookie counts and folder-depth derivation for internal tabs.
- Added regression tests for Derby internal streaming/materialized expressions and special-tab parsing.

## 0.1.1 (2026-03-16)
- Added chainable SQL query builder via `crawl.query(...)` with `select`, `where`, `group_by`, `having`, `order_by`, and `limit`.
- Implemented Derby GUI filter SQL for:
  - `Response Codes > Internal Redirect Chain`
  - `Hreflang > Not Using Canonical`
- Added behavioral Derby filter tests using an in-memory Derby-like fixture.

## 0.1.0
- Core loaders for CSV, SQLite, Derby `.dbseospider`, and `.seospider` via CLI.
- Generic tab access with GUI filter support (Derby + CSV fallback).
- Derby inlinks/outlinks and expanded link mappings.
- Crawl diff (`compare`) and raw SQL escape hatches (`raw`, `sql`).
