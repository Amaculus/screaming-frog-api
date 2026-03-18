# Changelog

## Unreleased
- Derby tab projection now skips literal `NULL` mapping columns at query time, which fixes oversized mapped tabs like `all_inlinks` / `all_outlinks` while still returning those fields as `None`.
- Derby `crawl.internal` now materializes expression-backed internal fields, including `Indexability` and `Indexability Status`.
- Derby filter compilation now supports mapped `db_expression` fields and post-filters mapped header/supplementary fields for `crawl.internal` and `crawl.tab(...)`.
- SQLite `crawl.internal` now follows the `internal_all` projection so mapped fields like `Indexability` / `Indexability Status` stay aligned with `crawl.tab("internal_all")`.
- Added Derby-backed special tab support for cookies, spelling/grammar, and structured-data summary/detail exports.
- Expanded mapping coverage and refreshed `schemas/mapping_nulls.md` / `schemas/inlinks_mapping_nulls.md`.
- Added direct Derby mappings for content language tabs, image alt-text length, and ten PageSpeed report savings columns.
- Added direct Derby mappings for `preload_key_requests_report`, `properly_size_images_report`, and mobile `PSI Request Status` fields.
- Added direct Derby mappings for content/internal readability metrics, near-duplicate fields, language/hash fields, and `mobile_all` PSI request status.
- Added direct Derby mappings for internal/url encoded-address, timestamp, response-time, and last-modified fields.
- Added derived Derby mappings for `Readability` across content and internal tabs using Screaming Frog's documented Flesch score groups.
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
