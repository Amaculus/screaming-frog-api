# Automated SEO Workflows

This file focuses on repeatable jobs you can schedule, rerun, and integrate into existing SEO operations.

## 1. Broken Internal Link Sweep
Use case:
- Run after every crawl.
- Produce a list of source pages that point to 4xx or 5xx internal destinations.
- Send the result to Slack, email, or a ticket queue.

Typical flow:
1. Load the crawl from `.dbseospider`.
2. Filter internal URLs for broken responses.
3. Pull inlinks for each broken destination.
4. Emit a flat table of `source -> destination -> anchor -> rel`.

Why this matters:
- It replaces manual clicking through the GUI.
- It turns a recurring cleanup task into a scheduled report.

## 2. Weekly Crawl Diff Review
Use case:
- Compare the latest crawl to last week's crawl.
- Highlight new broken URLs, title changes, canonical changes, and indexability changes.
- Push a short summary to a team channel.

Typical flow:
1. Load the previous crawl.
2. Load the current crawl.
3. Run `crawl.compare(...)`.
4. Serialize the change sets into a summary plus attachments.

Why this matters:
- It catches regressions before they accumulate.
- It gives engineering and SEO one shared change log.

## 3. Redirect Chain Watchlist
Use case:
- Generate redirect-chain inventories for important directories or templates.
- Alert when new chains appear or when an existing chain gets longer.

Typical flow:
1. Run the redirect chain helper or chain report tab.
2. Filter by source section or by hop count.
3. Compare against yesterday's result.

Why this matters:
- Redirect debt usually grows silently.
- A code workflow makes it cheap to monitor continuously.

## 4. Title And Meta QA Queue
Use case:
- Produce a working queue for content or SEO teams.
- Group pages with missing, duplicate, short, or overlong titles and descriptions.

Typical flow:
1. Query high-value title and description tabs.
2. Join with status, indexability, and template indicators.
3. Prioritize only indexable 200 pages.

Why this matters:
- It avoids wasting time on non-indexable or redirected URLs.
- It turns broad site audits into an ordered backlog.

## 5. Security Header Monitoring
Use case:
- Check CSP, HSTS, X-Frame-Options, and related headers on every crawl.
- Alert when a header disappears from protected areas.

Typical flow:
1. Query internal pages or header tabs.
2. Filter to key directories.
3. Diff against a baseline or policy allowlist.

Why this matters:
- These regressions are easy to miss manually.
- Crawl-based checks scale across the full site.

## 6. Accessibility Regression Sweep
Use case:
- Turn stored Axe results into a release-over-release report.
- Track issue counts, affected URLs, and sample pages.

Typical flow:
1. Query the accessibility summary tab.
2. Optionally join detail tabs for exact URLs and nodes.
3. Compare to the previous crawl.

Why this matters:
- Accessibility work becomes trackable like any other QA metric.

## 7. JS Rendering Drift Report
Use case:
- Detect pages where rendered HTML changes titles, H1s, descriptions, or directives.
- Route severe mismatches to engineering.

Typical flow:
1. Query the JavaScript comparison tabs.
2. Focus on templates where server-rendered parity matters.
3. Diff new issues against the previous crawl.

Why this matters:
- It exposes rendering-side regressions without browser scripting on your side.

## 8. Hreflang QA Job
Use case:
- Run scheduled checks for missing return links, noindex targets, non-canonical targets, and missing self-reference.

Typical flow:
1. Query `hreflang_*` tabs.
2. Group issues by locale, folder, or market.
3. Send per-market summaries to owners.

Why this matters:
- International SEO issues are high-impact and easy to miss without repeatable checks.

## 9. Internal Linking Opportunity Backlog
Use case:
- Identify pages that matter commercially but have weak internal link support.
- Turn the result into a content or merchandising task list.

Typical flow:
1. Pull inlink counts, unique inlinks, crawl depth, and indexability.
2. Optionally join analytics or business priority data.
3. Rank pages that deserve stronger internal linking.

Why this matters:
- This is easier to maintain as a recurring workflow than a one-off audit.

## 10. Raw SQL Escape Hatch Workflow
Use case:
- A power user needs a custom report before the typed layer covers it.
- The team cannot wait for a new abstraction.

Typical flow:
1. Run `crawl.sql(...)` directly against `APP.URLS`, `APP.LINKS`, or other Derby tables.
2. Serialize the result to CSV, DuckDB, Parquet, or Slack text.
3. Only later decide whether the query deserves a first-class helper.

Why this matters:
- The library stays useful even when mapping is incomplete.
- Advanced users can ship workflows immediately.
