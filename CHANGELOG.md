# Changelog

## Unreleased
- Implemented Derby-backed `Hreflang > Unlinked hreflang URLs` GUI filter support using `APP.LINKS` + `APP.INLINK_COUNTS`.
- Improved the DuckDB backend to resolve GUI-filtered tabs and `_all` base tabs like the Derby/CSV backends, and open `.duckdb` caches read-only.
- Changed DB-backed loaders to default to DuckDB-backed analysis (`.dbseospider`, `.seospider`, and DB crawl IDs), with Derby kept as the source-of-truth and DuckDB caches re-used automatically when the source has not changed.
- Moved key link-graph reports (`broken_inlinks_report`, `nofollow_inlinks_report`, `orphan_pages_report`) to DuckDB-first execution paths over raw `APP.LINKS`/`APP.UNIQUE_URLS`, so they no longer depend on materialized `all_inlinks` tabs in DuckDB caches.
- Added DuckDB-first execution for `broken_links_report()` and `summary()`, so lean DuckDB caches can still produce broken-page sampling and crawl-level rollups directly from raw relations plus `internal_all`.
- Added a DuckDB-first `compare()` path that projects only the internal fields required for diffing instead of loading full `internal_all` rows on both sides.
- Added a DuckDB-first `title_meta_audit()` path against `internal_all`, so missing title/meta checks no longer rely on materialized issue tabs in DuckDB caches.

## 0.2.0a1 (2026-03-21)
- Added DuckDB analytics-cache support:
  - `Crawl.from_duckdb(...)`
  - `crawl.export_duckdb(...)`
  - top-level `export_duckdb_from_derby(...)` / `export_duckdb_from_db_id(...)`
  - new `DuckDBBackend` for fast `pages()`, `links()`, `raw()`, `sql()`, `query()`, and chain-report workflows once a cache exists.
  - `tabs="all"` support to materialize every currently available mapped tab into the DuckDB cache.
  - direct loader/export flows for `.seospider` and DB crawl IDs via `backend="duckdb"` / `db_id_backend="duckdb"`.
- Added thin audit/report helpers on `Crawl`: `broken_links_report()`, `title_meta_audit()`, `indexability_audit()`, and `redirect_chain_report()`.
- Added graph-first audit helpers on `Crawl`: `broken_inlinks_report()`, `nofollow_inlinks_report()`, and `orphan_pages_report()`.
- Added issue-tab rollup helpers on `Crawl`: `security_issues_report()` and `canonical_issues_report()`.
- Added issue-tab rollup helpers on `Crawl`: `hreflang_issues_report()` and `redirect_issues_report()`.
- Added `crawl.summary()` for compact crawl-level monitoring counts across pages, links, issue rollups, and chains.
- Added ergonomic sitewide views: `crawl.pages()`, `crawl.links(direction=...)`, and `crawl.section(prefix)` for page/link workflows without remembering tab names.
- Added `crawl.search(...)`, per-view `search(...)`, and `CrawlSection.tab(...)` for page/link/tab text search and scoped generic-tab workflows.
- Added `collect()`, `first()`, `to_pandas()`, and `to_polars()` helpers on `InternalView` / `TabView`, plus dataframe exports on `QueryView` and `CrawlDiff`.
- Added `CrawlDiff.summary()` and `CrawlDiff.to_rows()` for flatter diff reporting and export workflows.
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
- Added exact `Occurrences` mappings for directive filter tabs using meta-robots, JS meta-robots, X-Robots-Tag, and `NUM_METAREFRESH`.
- Added bulk Derby mappings for internal performance, CrUX, semantic-similarity, text-ratio, link-score, and transfer/CO2 fields across internal, links, and validation tabs.
- Added internal redirect/blocked-resource carryover mappings for `Redirect URL` / `Redirect Type`, aligned redirect labels to `HTTP Redirect`, and filled internal PageSpeed issue summary text plus pending-link `Unlinked` flags.
- Added backend-derived redirect URL materialization from meta-refresh targets and HTTP `Location` headers, plus generic blob-derived cookie counts and folder-depth derivation for internal tabs.
- Added exact-safe hreflang multimap tab support for missing return links, inconsistent-language return links, non-canonical return links, and noindex return links.
- Added derived AJAX pretty/ugly URL mapping for JavaScript tabs and exact-safe `amphtml Link Element` extraction from stored original HTML.
- Added exact-safe mobile alternate extraction from stored original HTML and aligned `mobile_all` with the same derived mapping path as internal/mobile tabs.
- Mapped `Unlinked` on `all_inlinks` and hreflang non-200/unlinked URL reports from `APP.INLINK_COUNTS`.
- Added a generic supplementary-derived lookup path so tab fields can be derived from related encoded-URL tables without forcing CSV fallback.
- Mapped `orphan_pages.csv -> URL` to the inlink destination URL.
- Added CLOB-aware Derby row streaming and HTML-link extraction helpers to support exact derived fields from `APP.URLS.ORIGINAL_CONTENT`.
- Fixed Derby `crawl.internal` select-list overflow correctly by batching expression overflow queries per streamed chunk instead of preloading full-crawl overflow data into memory.
- Fixed SQLite and DuckDB row iteration paths to stream via `fetchmany()` instead of materializing full result sets with `fetchall()`, and fixed SQLite empty-sequence filters to compile to `1=0` instead of invalid `IN ()`.
- Tightened `schemas/mapping_nulls.md` / `schemas/inlinks_mapping_nulls.md` so they only report true unresolved `NULL` placeholders, not runtime/derived/blob-backed fields.
- Expanded the `use cases/` corpus with packaged workflow docs for agencies, ecommerce, publishing, migrations, multi-location, QA/governance, product ideas, data science, analytics joins, and operations integrations.
- Added persona/workflow indexes plus new packaged workflow notes for affiliate, directory/marketplace, newsroom, docs/API, franchise, regulated, education, and exec-scorecard use cases.
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
