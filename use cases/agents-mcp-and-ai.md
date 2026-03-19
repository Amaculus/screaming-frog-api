# Agents, MCP, and AI Workflows

This document collects the most useful ways to pair Screaming Frog crawl data with agents, MCP wrappers, and AI assistants. The goal is not just "chat over crawl data" but reliable workflows where an agent can inspect a crawl, branch into deeper analysis, assemble evidence, and hand back an actionable result.

## Core idea

Screaming Frog is strongest when the agent can see structured crawl state:
- crawl tables and tabs
- inlinks, outlinks, chains, and filters
- historical comparisons
- raw SQL escape hatches for the odd case

That makes it a good backend for:
- a conversational SEO copilot
- an internal analyst agent
- a crawl triage bot
- a QA reviewer for release or migration checks
- a knowledge base for site-specific operational facts

## MCP wrapper patterns

The strongest MCP pattern is to expose a narrow, opinionated tool layer on top of `Crawl` rather than handing an agent a raw database connection. The wrapper should speak in domain actions, not tables.

Good MCP tools:
- `list_crawls`
- `load_crawl`
- `get_tab`
- `filter_tab`
- `get_inlinks`
- `get_outlinks`
- `get_redirect_chain`
- `get_canonical_chain`
- `compare_crawls`
- `find_issues`
- `summarize_tab`
- `export_evidence`

### Why wrappers matter

An agent that can query anything can also ask the wrong thing. A wrapper can:
- keep the prompt small
- normalize tab names and columns
- hide backend differences between CSV, Derby, SQLite, and future warehouses
- enforce safe limits and row caps
- return evidence bundles instead of raw dumps

### Wrapper design ideas

- **Intent-based tools**: `find_broken_internal_links`, `show_pages_with_missing_title`, `compare_release_crawls`
- **Scoped queries**: tools that always require a crawl ID and a max row count
- **Evidence-first responses**: return summary + representative rows + links to raw rows
- **Policy-aware wrappers**: prevent exporting private URLs, PII-like parameters, or restricted sections without approval
- **Explanatory wrappers**: include "why this row matched" so the agent can justify the result

## Agentic tech audit workflows

This is the highest-value AI use case: an agent runs a structured technical audit and produces a report with evidence, severity, and next steps.

### 1) Release smoke audit

Scenario:
- A staging crawl finishes before a release.
- An agent compares it to the previous production crawl.
- It looks for status changes, redirect spikes, canonical changes, missing titles, new `noindex` pages, and indexability regressions.

Agent behavior:
- fetch summary counts by tab
- inspect outlier deltas
- pull representative URLs
- separate intentional changes from accidental regressions
- output a concise release risk list

### 2) Migration audit

Scenario:
- A site migrates from legacy URLs to a new architecture.
- An agent checks that legacy pages redirect cleanly, canonicals point correctly, and important templates preserved metadata.

Agent behavior:
- verify redirect chains are short
- detect loops and multi-hop chains
- inspect canonical clusters and canonicalized target pages
- confirm sitemap and internal-link alignment
- flag pages whose destination status is non-200

### 3) Content-quality audit

Scenario:
- Editorial wants a weekly QA pass on new landing pages.
- The agent checks title uniqueness, meta description completeness, H1 patterns, word count, headings structure, and structured data presence.

Agent behavior:
- compare content templates
- identify pages that violate the expected pattern
- surface examples from each content type
- suggest corrective actions in template language

## Internal knowledge tools

An agent becomes much more useful when it knows the site.

### Examples of knowledge tools

- "What are the highest-traffic template types?"
- "Which sections are most often noindexed?"
- "What pages share the same canonical target?"
- "Which directories have the most broken internal links?"
- "Which templates tend to miss structured data?"
- "What changed most between the last two crawls?"

### Knowledge sources to combine

- crawl tables
- previous crawls
- release notes
- CMS metadata
- analytics or GSC exports
- internal documentation about templates and ownership

### Knowledge-layer ideas

- page-type memory: the agent learns that `/blog/` is editorial, `/product/` is commercial, etc.
- owner memory: the agent links templates or folders to teams
- exception memory: the agent knows which pages are intentionally blocked, noindexed, or canonicalized
- glossary memory: the agent maps internal naming to crawl column names

## Agent-assisted workflows

### Triage assistant

The agent watches incoming crawl results and classifies issues:
- critical: 5xx, broken canonicals, indexation loss, redirect loops
- medium: title duplication, missing H1s, thin pages, inconsistent schema
- low: spacing, duplicate alt text, marginal content issues

It then produces:
- a priority queue
- suggested owners
- evidence links
- a short explanation for each issue

### SEO ops copilot

The agent helps day-to-day operations:
- answer "is this URL indexable?"
- explain why a page is non-indexable
- compare two versions of the same page
- identify which template change caused a regression
- draft QA checklists for a release

### Change-explainer bot

When something changes, the bot should explain:
- what changed
- how many URLs were affected
- whether the change is likely intentional
- which pattern or template seems responsible
- what to verify next

## Human-in-the-loop approval flows

The best agent workflow is not fully autonomous. It should:
- propose a diagnosis
- show evidence
- ask for approval on destructive or external actions
- record the final decision

Examples:
- "Apply the canonical fix to this template?"
- "Open a Jira ticket for these 114 URLs?"
- "Export the evidence bundle to the release channel?"
- "Escalate this crawl anomaly to engineering?"

## Future AI features

These are ideas that would require product work, but they fit the platform well:

- natural-language query layer over tabs and filters
- agent-generated audit plans based on site type
- automated templated QA playbooks
- LLM-powered issue clustering and deduplication
- evidence bundles with screenshots, snippets, and crawl rows
- knowledge graph of URLs, templates, owners, and issue histories
- cross-crawl conversational memory
- self-serve "ask the crawl" assistant for non-technical stakeholders

## Best-fit prompts

Examples of prompts an agent should handle well:
- "Find the top 20 technical regressions in the latest crawl."
- "Show pages whose canonical changed but the status stayed 200."
- "Compare this staging crawl to production and summarize risks."
- "Which page types are missing structured data?"
- "What URLs are orphaned but still important?"
- "Explain why these pages are excluded from indexation."

## Output expectations

An effective AI workflow should return:
- a short answer first
- evidence rows second
- grouped issues third
- clear next actions last

That keeps the result readable for analysts while still being precise enough for implementation teams.

