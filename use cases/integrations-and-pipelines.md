# Integrations And Pipelines

This file focuses on where the library fits inside broader data and automation systems.

## Common integrations

### Slack / Teams
- Post crawl diff summaries.
- Post broken-link counts after nightly crawls.
- Post launch QA pass/fail summaries.

### Jira / Linear / Asana
- Create remediation tickets from filtered crawl outputs.
- Route issues by template owner or market owner.
- Attach URLs, counts, and examples automatically.

### Data warehouses
- Load typed tabs into BigQuery, Snowflake, Postgres, or DuckDB.
- Join crawl state with analytics and Search Console.
- Maintain a historical page-state table.

### BI tools
- Feed Looker Studio, Power BI, Metabase, or Tableau.
- Build site health dashboards that refresh from crawl snapshots.
- Track issue counts and distributions over time.

### Content systems
- Feed optimization backlogs into Airtable or Notion.
- Sync title/meta queues into editorial workflows.
- Join crawl outputs with CMS ownership metadata.

### QA and release systems
- Run in GitHub Actions, GitLab CI, or Jenkins.
- Publish crawl summary artifacts into release pipelines.
- Attach regressions to release tickets automatically.

## Useful pipeline shapes

### Simple nightly pipeline
1. Crawl site.
2. Load `.dbseospider`.
3. Run high-signal checks.
4. Post summary and archive results.

### Diff pipeline
1. Fetch previous crawl from storage.
2. Load current crawl.
3. Run `crawl.compare(...)`.
4. Store diff JSON plus human-readable summary.

### Warehouse pipeline
1. Load selected tabs or raw SQL slices.
2. Normalize into warehouse tables.
3. Join with business dimensions.
4. Power dashboards and anomaly detection.

### Agent pipeline
1. Crawl or load a crawl.
2. Let an agent query typed tabs and raw SQL.
3. Generate a prioritized remediation memo.
4. Open tickets or update docs automatically.

## Design guidance
- Keep `.dbseospider` as the canonical crawl artifact.
- Export CSV only when another system truly requires it.
- Store crawl date, site, scope, and crawl ID with every downstream table.
- Treat typed mappings as stable interfaces and raw SQL as the escape hatch.
