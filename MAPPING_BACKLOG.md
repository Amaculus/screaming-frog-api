# Mapping Backlog

Current effective mapping coverage after the 2026-03-19 mobile-alternate pass:

- Field-level mapped coverage: `99.36%` (`15490 / 15589`)
- Fully mapped tabs: `95.7%` (`601 / 628`)
- Remaining effective gap cells: `99`

Effective coverage treats `runtime_extract`, `derived_extract`,
`multi_row_extract`, `blob_extract`, and `header_extract` as mapped.

This file is the working backlog for the remaining mapping work. It separates
exact-safe next batches from families that still need more evidence or backend
parsing.

## Completed Recently

- `multi_row_extract` backend support for repeated custom extraction matches
- `custom_extraction_all`, `internal_all`, `internal_html`, `all_inlinks`
  multi-match extractor rollout
- `change_detection_*` current-state carryovers plus crawl timestamp where exact
- `minimize_main_thread_work_report.csv` main-thread breakdown from
  `APP.PAGE_SPEED_API.JSON_RESPONSE`
- Exact `Occurrences` rollout for directive filter tabs
- Live smoke test harness for custom extraction multi-row materialization

## Exact-Safe Next

There is no remaining wide direct-column batch like custom extraction or
directive occurrences. The next defensible items are narrower validations.

### 1. `directives_outside_head.csv`

Only map `Occurrences` if a real count field exists in `APP.HTML_VALIDATION_DATA`
or another Derby table. Do not infer it from the boolean flag alone.

### 2. Potential Savings / Explanation carryovers

Remaining gaps:

- `all_image_inlinks.csv -> Potential Savings (bytes)`
- `all_inlinks.csv -> Potential Savings (bytes)`
- `incorrectly_sized_images.csv -> Potential Savings (bytes)`
- `content_not_sized_correctly_report.csv -> Explanation`

Only map once the exact PageSpeed source field or detail payload is verified.

### 3. Change Detection: Previous / Delta Side

Current-side fields are now mapped. Remaining gaps are:

- `Previous *`
- `Change`
- `Change %`
- `Similarity Match %`
- `Current/Previous Unique Types`

These should not be guessed from a single crawl.

## Needs Backend Parsing / More Work

These look possible, but not as a plain direct-column rollout.

### Structured Data Error Summary / Issue Tabs

- `structured_data_error_report.csv`
- `structured_data_error_summary_report.csv`
- `issues_overview_report.csv`

These need a stable issue dictionary / payload mapping, not raw blob passthrough.

### JavaScript Console / Target Size Detail Tabs

- `pages_with_javascript_issues.csv`
- `chrome_console_log_summary_report.csv`
- `target_size_report.csv`

These look runtime-parseable, but current local sample crawls do not persist the
needed populated payloads to prove exact-safe extraction.

### Accessibility Issue Metadata

Repeated null family:

- `Issue`
- `Guidelines`
- `User Impact`
- `Priority`
- `How To Fix`
- `Help URL`
- `Location on Page`
- `Issue Description`

These likely need a dedicated issue dictionary/parsing path rather than direct
table columns.

## Currently Blocked / Not Defensible Yet

Do not guess these.

- `Title 1 Pixel Width`
- `Meta Description 1 Pixel Width`
- `% of Total`
- `Carbon Rating`
- `Current/Previous Unique Types`
- `serp_summary.csv` duplicate `Character Length` / `Pixel Length` headers
- `crawl_overview.csv` row-oriented summary layout
- `change_detection_*` previous/delta values from a single crawl

## Operational Notes

- `multi_row_extract` is now the correct mechanism for repeated
  `APP.CUSTOM_EXTRACTION` / `APP.CUSTOM_JAVASCRIPT` matches when Derby stores
  one row per match.
- `Mobile Alternate Link` is now derived from `APP.URLS.ORIGINAL_CONTENT`
  wherever the tab can source HTML directly; `mobile_all.csv` uses a dedicated
  join-based runtime path because its base table is `APP.PAGE_SPEED_API`.
- Live probe note: Screaming Frog CSV exports can use the extraction name as the
  header (`Items 1`, `Items 2`, ...) while the typed Derby mapping currently
  exposes normalized generic slots (`Extractor 1 1`, `Extractor 1 2`, ...).
  That is acceptable for Derby-first access, but it is worth documenting if we
  want stricter CSV-header parity later.
