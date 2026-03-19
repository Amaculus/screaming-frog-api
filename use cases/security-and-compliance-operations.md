# Security And Compliance Operations

## Sitewide Security Checks
- Audit security headers across every crawlable page.
- Detect missing CSP, HSTS, X-Frame-Options, or X-Content-Type-Options coverage.
- Monitor mixed-status or insecure asset references discovered through crawls.
- Build recurring exception reports for security and platform teams.

## Compliance-Oriented QA
- Track cookie presence and cookie count changes over time.
- Surface pages serving unexpected headers or directives.
- Build evidence packs for recurring technical reviews.
- Separate template-level security regressions from isolated content issues.

## Release Monitoring
- Alert when critical headers disappear after deployments.
- Diff header behavior between baseline and current crawls.
- Track which sections are affected by a security regression.
- Prioritize remediation by exposure and URL volume.

## Coordination With Engineering
- Feed security-related crawl findings into ticketing or remediation queues.
- Attach raw crawl evidence to engineering issues.
- Provide section and template segmentation for faster root-cause analysis.
- Re-run the same checks after fixes without exporting anything manually.
