# Affiliate And Review Sites

## Core Workflows
- Audit money pages, comparison pages, and review pages for missing canonicals, redirect waste, and duplicate title patterns.
- Track expired offer URLs that still receive internal links.
- Validate affiliate outlinks, sponsored / nofollow usage, and destination response codes.
- Check review schema coverage, FAQ schema coverage, and rich-result eligibility by template.
- Compare crawls before and after commercial content updates to catch accidental noindex, title rewrites, or broken hubs.

## Automation Ideas
- Run a nightly broken-affiliate-link report and post failures to Slack.
- Export all external affiliate outlinks and join them to partner inventories.
- Flag pages with high inlink counts but weak titles, missing H1s, or non-indexable status.
- Detect clusters of near-duplicate review pages before they compete with each other.

## Why This Library Fits
- You can work directly from `.dbseospider` without hand-exporting every monetized URL segment.
- Inlinks, outlinks, response codes, canonicals, and diffing are already scriptable.
- Raw SQL gives fast access to custom partner-link audits before full mapping exists.
