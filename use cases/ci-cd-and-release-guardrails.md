# CI/CD And Release Guardrails

This file focuses on using crawls as a release-quality signal.

## Core pattern
1. Define a baseline crawl for a stable release.
2. Run a smoke crawl in staging or shortly after deployment.
3. Diff the new crawl against the baseline.
4. Fail, warn, or open tickets based on explicit thresholds.

## Guardrail patterns

### Canonical regression gate
- Block release when key templates gain mismatched canonicals.
- Block release when canonical targets become non-200 or non-indexable.
- Scope to directories like `/products/`, `/blog/`, or `/docs/`.

### Status-code regression gate
- Block release when new internal 4xx or 5xx pages appear.
- Warn when monitored pages move from 200 to redirect.
- Escalate when homepage, category pages, or top landing pages change status.

### Metadata parity gate
- Fail when titles or meta descriptions disappear on protected templates.
- Warn on unexpected bulk title changes outside an approved migration window.
- Compare HTML vs rendered HTML when JS hydration is part of the release.

### Hreflang parity gate
- Fail when missing return links appear for protected locales.
- Fail when noindex or non-canonical hreflang targets are introduced.
- Use locale owners as routing destinations for alerts.

### Internal link regression gate
- Warn when unique inlinks drop sharply for priority pages.
- Fail when critical templates lose navigation links.
- Track crawl depth shifts as a lightweight discovery signal.

### Structured-data gate
- Fail when eligible rich-result templates lose eligibility.
- Warn on newly introduced validation errors.
- Use detail tabs for exact issue messages when available.

### Accessibility gate
- Fail on new high-severity accessibility violations.
- Warn on aggregate increases in affected URLs.
- Attach sample URLs for fast engineering follow-up.

## Output formats
- PR comment with a short diff summary.
- JSON artifact for CI systems.
- Slack summary for QA and SEO.
- Jira or Linear issue creation for durable tracking.

## Why this works
- Crawl data catches release problems that unit tests and visual tests miss.
- The library makes the workflow scriptable instead of GUI-bound.
- The same checks can run every release without extra analyst effort.
