# Lazy DuckDB Cache Policy

Date: 2026-05-22

This note defines the intended performance direction for normal library use, not only agent precompute.

## Goal

Use source backends as the source of truth and DuckDB as the fast analytical working set.

`Crawl.load(...)` should stay cheap. Common reads should then use narrow persisted helper relations when possible instead of materializing full Screaming Frog export tabs.

## Helper Relations

- `internal_basic`: minimal internal URL shape, currently `Address` and `Status Code`.
- `internal_common`: common page fields used by audits, response-code samples, summaries, and projections.
- `links_core`: common link fields used by inlink/outlink views and link-tab projections.

## Preferred Execution Order

For counts:

1. Use an existing materialized tab relation if available.
2. Use a helper relation when the helper can answer exactly.
3. Delegate to the lazy source backend fast path when available.
4. Materialize the full tab only when exact helper/source paths cannot answer.
5. Fall back to iterator counting only as a compatibility path.

For bounded rows:

1. Use an existing materialized tab relation with `LIMIT`.
2. Use a helper relation when the requested output shape is compatible.
3. Delegate to source backend `tab_rows(...)` when available.
4. Materialize or iterate only when needed.

For full `tab(...).collect()`:

1. Preserve exported tab output shape.
2. Use helper relations only when the helper output is compatible with the tab shape.
3. Do not silently return extra or missing columns for full generic tab collection.

For projections:

1. Prefer `tab_select(...)` over full-row iteration.
2. Use helper relations if all requested columns and filters are supported.
3. Fall back to projecting rows from `get_tab(...)` if no backend projection is available.

## Guardrails

- Fast paths must preserve exact counts.
- Full tab collection must preserve output shape.
- Helper-backed projections may return only requested columns.
- Persisted helper relations must be schema-versioned and rebuilt when helper semantics change.
- Source-of-truth overrides must be explicit backend capabilities, not backend class-name checks.
- If a filter cannot be represented in SQL/helper form, fall back instead of guessing.
- New helper shortcuts should include tests proving the full tab relation was not materialized.
