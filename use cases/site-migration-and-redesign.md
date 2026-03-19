# Site Migration And Redesign

This file focuses on high-risk projects where crawl parity matters.

## Pre-launch checks
- Baseline crawl of the current site.
- Redirect mapping validation.
- Canonical parity validation.
- Template metadata parity.
- Internal-link graph snapshots for key sections.

## Launch-day checks
- New 4xx and 5xx pages.
- Redirect chains or loops.
- Missing templates or dropped sections.
- Canonical target failures.
- Unexpected noindex or robots blocks.

## Post-launch follow-up
- Compare crawl depth shifts.
- Compare unique inlinks to strategic pages.
- Compare metadata changes by template.
- Compare hreflang integrity by locale.
- Track issue burn-down over the first weeks after launch.

## Why code beats manual exports here
- Migration QA is repetitive and time-sensitive.
- You often need to rerun the same checks multiple times per day.
- Diff-driven workflows are easier to trust than ad-hoc spreadsheet work.
