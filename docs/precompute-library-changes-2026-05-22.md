# Precompute Support Library Changes

Date: 2026-05-22

Context: `auditlabs-query-agent` now precomputes all native diagnostics during `prepare`. The blocking performance issue was repeated full-tab iteration just to answer two simple questions:

1. How many rows are in this tab after filters?
2. What are the first N rows after filters?

The library changes below add exact fast paths for those operations so the query agent can precompute all diagnostics without repeatedly materializing whole tabs.

## Files Changed

- `screamingfrog/backends/base.py`
- `screamingfrog/backends/derby_backend.py`
- `screamingfrog/backends/duckdb_backend.py`
- `screamingfrog/crawl.py`

## Summary

- Added backend-level APIs for exact tab counts and limited row fetches.
- Exposed those APIs on `Crawl`.
- Implemented DuckDB-native `COUNT(*)` and `LIMIT` queries where possible.
- Implemented Derby-native count and row-limit paths where possible.
- Kept safe fallbacks to existing iterator-based behavior for complex/special tabs.

## Detailed Changes

### `screamingfrog/backends/base.py`

Added two backend methods:

- `tab_count(tab_name, filters=None) -> int`
- `tab_rows(tab_name, limit, filters=None) -> list[dict[str, Any]]`

These are intentionally narrow primitives:

- `tab_count` returns the exact filtered row count.
- `tab_rows` returns the first `N` filtered rows without forcing the caller to consume the entire tab.

The base implementation raises `NotImplementedError`, so each backend can opt in explicitly.

### `screamingfrog/crawl.py`

Added public convenience methods on `Crawl`:

- `Crawl.tab_count(...)`
- `Crawl.tab_rows(...)`

Behavior:

- If the active backend implements the fast path, `Crawl` delegates directly.
- Otherwise it falls back to the old iterator behavior:
  - `tab_count`: `sum(1 for _ in ...)`
  - `tab_rows`: `islice(...)`

This preserves compatibility for backends that do not yet implement the new APIs.

### `screamingfrog/backends/duckdb_backend.py`

Added exact fast paths for DuckDB-backed tabs.

#### `tab_count(...)`

- Resolves the tab relation in DuckDB.
- Reuses the existing filter-to-SQL machinery.
- Executes `SELECT COUNT(*) FROM (...)`.
- Falls back to source-backend iteration if post-filters prevent a pure SQL count.
- If the relation is missing, tries the lazy source backend or `ensure_tab(...)` before failing.

#### `tab_rows(...)`

- Resolves the tab relation in DuckDB.
- Reuses the existing filter SQL.
- Executes `SELECT * FROM (...) LIMIT ?`.
- Returns rows as dictionaries using cursor metadata.
- Falls back to source-backend iteration if post-filters prevent a pure SQL limited fetch.

Why this mattered:

- For precompute, the query agent only needed exact counts and a small evidence sample from many report tabs.
- Counting or sampling through the old iterator path caused repeated expensive tab scans.

### `screamingfrog/backends/derby_backend.py`

Added Derby-side support for exact counts and bounded row fetches, with careful fallback behavior.

#### `get_tab(...)`

Added support for an internal `__limit__` filter key:

- Pops `__limit__` before normal filter processing.
- Appends `FETCH FIRST <n> ROWS ONLY` to generic SQL-backed tab queries.

This allows `tab_rows(...)` to reuse the existing query-building path instead of duplicating the full tab SQL assembly logic.

#### `tab_count(...)`

Implemented exact count behavior where it is safe and cheap:

- Special-cased hreflang multimap tabs to count directly from the underlying multimap tables.
- For generic SQL-backed tabs, builds the same WHERE/JOIN structure and runs `SELECT COUNT(*)`.
- If a tab requires post-filtering, blob checks, or other special iterator-only logic, falls back to iterating `get_tab(...)`.
- Returns `0` for missing backing tables instead of forcing expensive failures.

#### `tab_rows(...)`

Implemented limited row fetches:

- Generic SQL-backed tabs use the new `__limit__` path.
- Special tabs fall back to iteration and stop once `limit` rows have been collected.

Why this mattered:

- Some of the query-agent diagnostics still reach Derby-backed tabs.
- The agent needed exact counts and bounded evidence without pulling every row into Python.

## Key Performance Observation

The most important profiling result was that precompute did **not** need tab columns, but it did need exact counts and limited rows. On the WorkflowMax crawl:

- exact count on a problematic tab was fast
- limited row fetch was acceptable
- column introspection was extremely slow

So the library work focused on enabling the query agent to avoid whole-tab scans and to stay on exact counts/small samples where the diagnostic payload actually uses them.

## Compatibility Notes

- The new APIs are additive.
- Existing callers of `Crawl.tab(...)` are unaffected.
- Backends that do not implement the new methods continue to work through the fallback logic in `Crawl`.

## Consumer

These changes are consumed by `auditlabs-query-agent` in its precompute path:

- `runtime.collect_tab(...)` now uses exact `tab_count(...)`
- `runtime.collect_tab(...)` now uses bounded `tab_rows(...)`
- this lets `prepare` precompute all native diagnostics without repeated full report scans
