# Changelog

## Unreleased
- Derby `crawl.internal` now materializes expression-backed internal fields, including `Indexability` and `Indexability Status`.
- Derby filter compilation now supports mapped `db_expression` fields and post-filters mapped header/supplementary fields for `crawl.internal` and `crawl.tab(...)`.
- SQLite `crawl.internal` now follows the `internal_all` projection so mapped fields like `Indexability` / `Indexability Status` stay aligned with `crawl.tab("internal_all")`.
- Added Derby-backed special tab support for cookies, spelling/grammar, and structured-data summary/detail exports.
- Expanded mapping coverage and refreshed `schemas/mapping_nulls.md` / `schemas/inlinks_mapping_nulls.md`.
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
