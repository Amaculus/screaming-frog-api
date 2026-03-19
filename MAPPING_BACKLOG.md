# Mapping Backlog

Current mapping coverage after the 2026-03-19 directives-occurrence pass:

- Field-level mapped coverage: `94.0%` (`14659 / 15589`)
- Fully mapped tabs: `74.2%` (`466 / 628`)
- Remaining literal `NULL` cells: `930`

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

### 2. Potential Savings on inlink tabs

Remaining gaps:

- `all_image_inlinks.csv -> Potential Savings (bytes)`
- `all_inlinks.csv -> Potential Savings (bytes)`

Only map once the exact PageSpeed source field is verified.

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

### Structured Data / Validation Detail Tabs

- `jsonld_urls_detailed_report.csv`
- `microdata_urls_detailed_report.csv`
- `rdfa_urls_detailed_report.csv`
- `validation_errors_detailed_report.csv`
- `validation_warnings_detailed_report.csv`
- `google_rich_results_features_report.csv`
- `google_rich_results_features_summary_report.csv`

These need real payload parsing, not blob passthrough.

### Chain Reports

- `redirects.csv`
- `redirect_chains.csv`
- `redirect_and_canonical_chains.csv`
- `canonical_chains.csv`

Remaining gaps are concentrated here. Most are chain-detail fields, not simple
carryovers.

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
- `Mobile Alternate Link`
- `amphtml Link Element`
- `Current/Previous Unique Types`
- generic rich-result validation issue details without confirmed Derby source

## Operational Notes

- `multi_row_extract` is now the correct mechanism for repeated
  `APP.CUSTOM_EXTRACTION` / `APP.CUSTOM_JAVASCRIPT` matches when Derby stores
  one row per match.
- Live probe note: Screaming Frog CSV exports can use the extraction name as the
  header (`Items 1`, `Items 2`, ...) while the typed Derby mapping currently
  exposes normalized generic slots (`Extractor 1 1`, `Extractor 1 2`, ...).
  That is acceptable for Derby-first access, but it is worth documenting if we
  want stricter CSV-header parity later.
