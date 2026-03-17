# Changelog

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
