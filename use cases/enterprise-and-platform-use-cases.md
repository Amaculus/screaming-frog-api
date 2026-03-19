# Enterprise and Platform Use Cases

This document focuses on how Screaming Frog data can fit into larger organizations: shared governance, repeatable QA, platform operations, and multi-team workflows. The emphasis is on operational reliability, auditability, and scale.

## What enterprises need

Enterprise SEO teams usually need more than a crawl export. They need:
- consistent QA across many sites or locales
- release governance for changing templates
- repeatable checks that can be delegated
- data that can flow into warehouses, BI tools, tickets, and alerting
- traceability from issue to evidence to owner
- a way to support many stakeholders without giving everyone direct access to crawl files

## Multi-crawl governance

Multi-crawl governance means treating crawls as managed assets.

### Common governance patterns

- **Pre-release vs post-release**: compare staging and production crawls before a rollout
- **Weekly baseline**: use one crawl as the canonical reference point for trend analysis
- **Template watchlist**: monitor important template groups independently
- **Market-level governance**: compare country sites, language subfolders, or brand properties
- **Program-level governance**: keep a recurring schedule for audits across a portfolio

### Governance checks

- critical status changes
- redirect target changes
- canonical target drift
- indexability loss
- template-wide title/meta regressions
- broken internal navigation patterns
- structured data regressions
- hreflang inconsistencies

### Operational policy ideas

- crawl naming conventions
- minimum evidence retention periods
- issue severity standards
- ownership routing rules
- approval flows for large changes
- change freeze windows before major releases

## Platform and shared-service use cases

Large teams often need a platform, not a one-off analysis.

### 1) SEO QA platform

The crawl backend becomes the engine behind a QA platform that checks:
- page titles
- meta descriptions
- headings
- canonicals
- redirects
- structured data
- image alt text
- pagination
- hreflang
- indexability

This can run as:
- a scheduled batch job
- a CI/CD step
- a manual review service
- a dashboard for content and engineering teams

### 2) Internal SEO data service

Instead of exporting files by hand, the crawl data can be served as a reusable service:
- query by site, crawl ID, folder, template, or issue type
- pull evidence into BI or ticketing systems
- expose stable views for analysts
- support internal self-service

### 3) Centralized crawl governance

For organizations with many properties, the platform can standardize:
- crawl templates
- export profiles
- checks and thresholds
- naming conventions
- issue taxonomy
- reporting cadence

## Templated QA

Templated QA is one of the best enterprise use cases because it turns a crawl into a rules engine.

### Example template checks

- category pages must have unique titles and one H1
- product pages must include canonical, schema, and valid internal links
- blog posts must have article schema, descriptive meta, and no accidental noindex
- faceted pages must follow indexation rules
- locale pages must have correct hreflang and canonical behavior

### How templated QA works

1. identify a page type or folder
2. apply a checklist
3. compare against expected patterns
4. return only exceptions
5. create ticket-ready evidence

### Why it matters

- content teams get a repeatable standard
- engineering gets clear templates to fix
- SEO leads can scale QA across many releases

## Marketplace and partner ideas

The platform can support a broader ecosystem.

### Possible marketplace concepts

- prebuilt audit packs for common site types
- importable rule sets for regulated industries
- custom connectors for BI, Jira, Slack, and warehouse tools
- template-specific QA bundles
- partner-maintained analysis add-ons
- "crawl recipes" for common workflows

### Commercial opportunities

- agency-grade audit kits
- enterprise onboarding packages
- vertical-specific compliance checks
- managed crawl review services
- premium anomaly detection and alerting

## Warehouse and BI workflows

Many enterprises already have a warehouse. Crawl data should fit there cleanly.

### Useful warehouse patterns

- land crawl tables in DuckDB for local analytics
- push normalized crawl facts into Snowflake, BigQuery, Redshift, or Postgres
- model one row per URL per crawl
- store issue facts separately from page facts
- preserve crawl metadata and run IDs
- maintain a history table for deltas over time

### BI questions this enables

- which templates regress most often?
- which teams own the most recurring issues?
- how many pages are affected by each issue family?
- what is the median time to resolve a crawl issue?
- which changes correlate with traffic or conversion shifts?

### Operational warehouse ideas

- semantic layer for URL, template, and issue dimensions
- scheduled refreshes after each crawl
- data quality checks on the crawl ingestion itself
- executive dashboards for site health

## Enterprise reporting

Executives and non-SEO stakeholders usually need simpler outputs.

### Good reporting formats

- site health scorecards
- issue trend charts
- release readiness summaries
- folder-level KPI snapshots
- top exceptions by severity
- owner-based task lists

### Audience-specific views

- **Engineering**: exact URLs, redirect chains, status codes, fix instructions
- **Content**: titles, meta, headings, schema, content quality
- **Product**: launch risk, template drift, conversion-impacting regressions
- **Leadership**: trend lines, risk concentration, release confidence

## Cross-functional operating model

Enterprise SEO works best when crawl data is part of the operating system.

### Example flows

- a release triggers a crawl
- the crawl is compared to the baseline
- the platform assigns issues to owners
- tickets are opened automatically
- status is tracked until closure
- the next crawl confirms remediation

### Supporting roles

- SEO lead defines the policy
- engineering owns technical fixes
- content owns metadata and on-page content
- data/BI owns reporting and pipelines
- ops owns the crawl schedule and alerting

## Future platform features

These ideas would deepen enterprise value:

- multi-site crawl orchestration
- role-based access controls for sensitive crawl data
- workflow approvals for crawl exports and issue escalations
- automated routing of issues to owners
- SLA tracking for issue remediation
- reusable governance rule packs
- crawl comparison at portfolio scale
- integrations with ticketing, chat, and observability systems
- change management dashboards tied to crawl evidence

## Best-fit enterprise scenarios

- a retailer with many locales and template families
- a publisher with frequent content and schema changes
- a SaaS company with release-heavy product pages
- a marketplace with massive faceted navigation
- a franchise or franchise-like network of regional sites
- an agency managing dozens of client properties

## Why the platform story matters

The core value is not just "can we inspect a crawl?" It is "can we make crawl data reliable enough to run SEO operations?"

That means:
- standardized checks
- repeatable workflows
- evidence retention
- integration with business systems
- team ownership
- trend monitoring over time

