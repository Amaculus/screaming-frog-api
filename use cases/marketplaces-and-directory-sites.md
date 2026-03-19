# Marketplaces And Directory Sites

## Core Workflows
- Audit faceted navigation for crawl traps, duplicate URL parameters, and unnecessary indexable combinations.
- Monitor category, location, and listing pages for title collisions and canonical drift.
- Find orphaned listings and stale inventory pages that lost internal discovery paths.
- Trace redirect chains caused by listing migrations or slug normalization.
- Measure internal link distribution from hubs to child listings and back.

## Automation Ideas
- Build a weekly report of new orphan pages by directory or marketplace segment.
- Alert when a location or category hub falls out of the internal graph.
- Compare crawl snapshots after feed or CMS releases to spot deleted listing paths.
- Create SQL-driven audits for status-code changes on top revenue categories.

## Why This Library Fits
- Directory and marketplace sites generate huge link graphs; Derby-backed querying avoids repetitive export loops.
- `crawl.compare()` is useful for inventory churn and template regressions.
- Raw SQL helps when you need ad hoc segmentation by path, parameter, or response state.
