# API Products And Developer Docs

## Core Workflows
- Audit docs sites for broken internal links across versioned documentation.
- Track redirect chains after docs URL migrations or version deprecations.
- Validate canonical rules across current and legacy docs sections.
- Detect orphan API reference pages and weak navigation paths.
- Monitor response codes, title patterns, and heading coverage across generated docs.

## Automation Ideas
- Add a release-gate audit for docs deploys: 4xx pages, canonical mismatches, missing titles, and broken nav links.
- Diff the docs crawl every release to spot accidental path deletions.
- Build package-version dashboards from raw crawl SQL and path conventions.
- Join crawl data with docs analytics to prioritize fixes on high-traffic endpoints.

## Why This Library Fits
- Developer docs often need CI-friendly checks rather than GUI review.
- The link graph is useful for validating generated sidebars, breadcrumbs, and version hubs.
- SQL and Python make it straightforward to segment by product, version, or locale.
