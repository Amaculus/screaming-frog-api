# Master List

This is the broad inventory. It is intentionally expansive. Some are ready today, some are thin wrappers around existing methods, and some are roadmap ideas that become practical once more tab families are mapped.

## Packaged Workflow Notes
- `advanced-analysis.md`
- `agency-and-freelancer-operations.md`
- `agents-mcp-and-ai.md`
- `automated-seo-workflows.md`
- `ci-cd-and-release-guardrails.md`
- `client-delivery-and-reporting.md`
- `content-and-information-architecture.md`
- `crawl-ops-and-infrastructure.md`
- `data-platform-and-warehousing.md`
- `digital-pr-and-linkable-assets.md`
- `ecommerce-and-marketplaces.md`
- `b2b-and-enterprise-sites.md`
- `enterprise-and-platform-use-cases.md`
- `experimentation-and-forecasting.md`
- `integration-recipe-snippets.md`
- `integrations-and-pipelines.md`
- `knowledge-management-and-training.md`
- `local-seo-and-multi-location.md`
- `mcp-and-product-concepts.md`
- `migration-war-room.md`
- `monitoring-and-alerting.md`
- `notebooks-and-data-science.md`
- `python-recipe-snippets.md`
- `productized-services-and-agents.md`
- `publishing-and-programmatic-seo.md`
- `quality-assurance-and-governance.md`
- `saas-and-product-led-growth.md`
- `search-console-and-analytics-joins.md`
- `security-and-compliance-operations.md`
- `structured-data-and-rich-results-programs.md`
- `site-migration-and-redesign.md`
- `ticketing-and-ops-integrations.md`

## Crawl Intake And Normalization
- Load a `.dbseospider` crawl and expose typed internal pages.
- Load a crawl by DB id and query it without opening the GUI.
- Run raw Derby SQL against crawl tables for custom analysis.
- Use `crawl.raw()` to inspect unmapped tables immediately.
- Convert headless CLI crawl output into a DB-backed analysis artifact.
- Standardize crawl intake for agencies with repeatable loading code.
- Build a local crawl catalog keyed by client, date, and crawl scope.
- Validate that a crawl contains the required tables before analysis starts.
- Build known-good crawl smoke tests for client templates.
- Create a data-ingestion contract around `.dbseospider` files in shared storage.
- Attach crawl metadata to downstream jobs before analytics runs.
- Normalize crawl snapshots into a versioned archive for longitudinal analysis.

## Audit Automation
- Generate broken internal link reports on every crawl.
- Flag internal 4xx and 5xx pages automatically.
- Surface redirect chains without manual exports.
- Surface canonical chains without manual exports.
- Flag pages with mismatched canonicals and canonically broken destinations.
- Audit indexability at scale from typed internal fields.
- Audit title tags and meta descriptions for missing, duplicate, or outlier patterns.
- Audit heading coverage and non-sequential heading usage.
- Audit response times and page-size outliers.
- Audit security headers across all crawled URLs.
- Audit cookie usage across a site without browser automation.
- Audit accessibility violations from stored Axe results.
- Audit structured-data coverage and rich-result eligibility.
- Audit language, spelling, and grammar signals at page level.
- Audit near-duplicate and semantically similar pages.
- Audit redirect targets that resolve to non-indexable pages.
- Audit pages blocked by robots but still linked internally.
- Audit noindex pages that still receive strong inlink signals.
- Audit thin content using word count, readability, and semantic relevance together.
- Audit JS-rendered metadata changes against raw HTML metadata.

## Internal Linking And Graph Workflows
- Find every page linking to a 404 destination.
- Find every page linking to a redirected destination.
- Find all inlinks with `rel=nofollow`.
- Find all outlinks marked sponsored or UGC.
- Compare internal vs external outlink distribution by template or directory.
- Find orphan pages from crawl-only data and auxiliary discovery sources.
- Build internal link distribution models by crawl depth.
- Rank pages by inlinks, unique inlinks, and link score together.
- Identify overlinked utility pages and underlinked money pages.
- Audit image links separately from text links.
- Audit anchor text patterns to specific landing pages.
- Detect overuse of generic anchors like `click here`.
- Find internal links to canonicalized or non-indexable targets.
- Find rel prev/next patterns and pagination edge cases.
- Build path-based link maps between site sections.
- Analyze footer and navigation link saturation across templates.
- Compare internal link graph changes between crawls.
- Detect link equity leakage into redirect chains.
- Detect high-value pages with declining unique inlinks over time.
- Build content hub / spoke validation checks from inlink data.

## Technical Monitoring And Alerting
- Run nightly crawl diffs and alert on regressions.
- Alert when status codes change for key pages.
- Alert when page titles change on critical templates.
- Alert when canonicals change unexpectedly.
- Alert when indexability flips on monitored URLs.
- Alert when internal outlinks collapse after a deploy.
- Alert when unique inlinks drop for strategic pages.
- Alert when meta robots directives change.
- Alert when response times spike beyond a threshold.
- Alert when structured-data issues appear on product or article templates.
- Alert when accessibility violations increase release-over-release.
- Alert when security headers disappear.
- Alert when JS-rendered metadata diverges from HTML output.
- Alert when PageSpeed opportunity counts jump across a section.
- Alert when hreflang relationships break.
- Alert when new redirect or canonical chains appear.
- Alert when crawl depth increases for target pages.
- Alert when previously indexable URLs become blocked by robots or noindex.

## CI/CD And Release Guardrails
- Fail a deployment if new 4xx internal pages appear.
- Fail a deployment if canonicals break on protected templates.
- Fail a deployment if monitored titles or descriptions are removed.
- Fail a deployment if hreflang reciprocity breaks.
- Fail a deployment if robots directives regress.
- Fail a deployment if structured-data eligibility drops.
- Fail a deployment if key pages lose internal links.
- Run smoke crawls after staging deploys and diff against baseline.
- Gate releases on redirect-chain thresholds.
- Gate releases on accessibility regressions.
- Gate releases on PageSpeed opportunity regressions.
- Gate releases on orphan-page creation in navigation-critical sections.
- Snapshot crawl metrics per commit or release tag.
- Attach crawl-diff summaries to pull requests.
- Publish crawl-quality badges inside internal engineering dashboards.

## Reporting And Client Delivery
- Produce recurring client health reports from code, not spreadsheets.
- Generate prioritized issue summaries by site section.
- Build monthly `what changed` reports from crawl diffs.
- Build board-level KPI summaries from technical crawl metrics.
- Feed Looker Studio or BI dashboards from extracted crawl tables.
- Produce before-and-after migration QA reports.
- Generate sitewide redirect-chain inventories for remediation projects.
- Generate page-title and meta-description optimization backlogs.
- Produce internal-linking opportunity reports for content teams.
- Produce issue-specific export packs only when needed.
- Deliver white-label automated audit summaries.
- Build SLA-style technical monitoring reports for retainers.
- Create per-country localization quality summaries.
- Create template-level issue rollups for engineering planning.
- Build top-regressions-this-week summaries for stakeholders.

## Content Operations
- Find pages with missing or weak titles and descriptions.
- Find content clusters with overlapping semantic intent.
- Find thin pages in high-priority sections.
- Find pages whose H1, title, and canonical signals do not align.
- Find pages with stale or template-only metadata.
- Find content areas with weak internal link support.
- Build rewrite queues from readability, thin-content, and duplicate signals.
- Detect placeholder or lorem ipsum content before publication.
- Identify underlinked evergreen assets worth promotion.
- Identify cannibalization candidates from near-duplicate and semantic similarity data.
- Segment content inventories by quality bands for editorial planning.
- Build refresh candidates from declining link support and poor metadata.

## Migrations, Redesigns, And QA
- Validate redirect mapping after a site migration.
- Verify canonical consistency across migrated templates.
- Compare pre and post migration internal link counts.
- Compare pre and post migration crawl depth distribution.
- Detect pages lost between legacy and new crawls.
- Detect pages added unexpectedly during rollout.
- Verify metadata parity after CMS migration.
- Verify hreflang parity after domain or locale migration.
- Verify image, CSS, and JS asset stability after replatforming.
- Validate mobile alternate and AMP relationships where relevant.
- Produce issue burn-down lists for launch war rooms.
- Create migration sign-off checklists backed by crawl data.

## International SEO And Localization
- Audit hreflang coverage and reciprocity.
- Audit missing self-reference and missing x-default patterns.
- Audit language mismatches in return links.
- Audit noindex and non-canonical hreflang targets.
- Compare localized template completeness across markets.
- Detect missing localized metadata fields.
- Monitor country-folder or subdomain parity over time.
- Build market-by-market issue rollups.
- Detect untranslated or copied content with similarity metrics.
- QA localization launches using crawl diffs.

## PageSpeed, Rendering, And Frontend QA
- Extract PageSpeed opportunities directly from stored API payloads.
- Compare HTML vs rendered HTML for metadata drift.
- Detect JS-only titles, H1s, or meta descriptions.
- Audit blocked JS resources that break rendering.
- Audit excessive DOM size and layout-shift detail rows.
- Audit font legibility and viewport issues at scale.
- Surface cache-policy and legacy-JS opportunities per resource.
- Track PageSpeed opportunity counts over time.
- Validate frontend performance regressions after releases.
- Build frontend QA backlogs tied to exact URLs and resources.

## Security, Compliance, And Accessibility
- Verify presence of security headers sitewide.
- Find pages missing CSP, HSTS, X-Frame-Options, or similar headers.
- Monitor cookie usage and cookie drift over time.
- Review accessibility violations grouped by rule and impact.
- Produce accessibility summaries for legal and compliance teams.
- Compare accessibility issue counts between releases.
- Track structured-data validation issues as compliance-style checks.
- Use crawl archives as evidence for technical due diligence.

## Data Engineering And Warehousing
- Export crawl slices into DuckDB, Parquet, or warehouse tables.
- Join crawl data with analytics, Search Console, logs, and CRM data.
- Create a slowly changing dimension of page technical state over time.
- Build template-level fact tables from page attributes.
- Materialize crawl graph tables for downstream analytics.
- Feed feature stores for SEO forecasting or prioritization models.
- Build warehouse-backed SEO scorecards.
- Use crawl data as one source in broader site observability pipelines.
- Store crawl diffs as event streams for downstream consumers.
- Build QA marts for engineering teams from filtered crawl subsets.

## AI, Agents, Skills, And MCP
- Let an agent answer technical-audit questions from a crawl DB.
- Use the library as the crawl-data backend for an MCP server.
- Build Claude Code or Codex skills that inspect crawl files directly.
- Generate narrative audit summaries from typed fields plus raw SQL.
- Build agent workflows that crawl, diff, and open Jira tickets.
- Build agents that propose redirect rules from migration diffs.
- Build agents that investigate broken internal links and suggest fixes.
- Build agents that write section-specific remediation briefs.
- Build agents that monitor crawl regressions and post to Slack.
- Create conversational interfaces for non-technical stakeholders to ask crawl questions.
- Create AI-assisted QA flows for launch checklists.
- Create agentic SEO copilots that query raw Derby when mappings are missing.

## Productized Services And Internal Tools
- Turn recurring audits into a productized monthly service.
- Build a crawl-triage app on top of typed tabs and raw SQL.
- Build internal dashboards for account managers and SEO leads.
- Build self-serve QA checkers for developers before release.
- Build a client portal showing crawl health trends.
- Build a migration QA toolkit with reusable recipes.
- Build a SaaS wrapper that ingests `.dbseospider` files and runs standard checks.
- Build template-governance tools for large content organizations.
- Build enterprise workflow connectors into Jira, Linear, Asana, or Notion.
- Build SEO ops playbooks that execute from one CLI command.

## Vertical-Specific Workflows
- Ecommerce collection and product QA.
- News and publishing freshness / archive QA.
- Marketplace category and filter crawl governance.
- SaaS docs-site release QA.
- Franchise and location-page consistency audits.
- Real-estate inventory and faceted-navigation QA.
- Travel and hospitality internationalization audits.
- Healthcare compliance-heavy accessibility and metadata checks.
- Finance and legal high-trust page monitoring.
- Media-site ad-tech and render-regression QA.

## Research And Reverse-Engineering
- Use raw SQL to discover fields Screaming Frog does not expose in the GUI.
- Compare Derby tables to CSV schemas to find hidden data.
- Reverse-engineer computed fields for parity with GUI outputs.
- Identify which tabs are direct DB projections vs computed reports.
- Build coverage dashboards for mapped vs unmapped fields.
- Validate assumptions about stored vs derived crawl data.
- Prototype new library features straight from raw DB tables before adding typed abstractions.
