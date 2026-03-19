# Monitoring And Alerting

This file focuses on persistent monitoring loops rather than one-off audits.

## Monitoring model
- Crawl on a schedule.
- Materialize a small set of important signals.
- Compare to the previous crawl or a stable baseline.
- Alert only on meaningful changes.

## High-signal alert types

### Status changes
- 200 -> 404
- 200 -> 500
- 200 -> redirect
- redirect -> 404
- Any status drift on protected URLs

### Indexability changes
- Indexable -> Non-Indexable
- Canonicalized unexpectedly
- Blocked by robots unexpectedly
- New noindex directives on production pages

### Metadata changes
- Title removed
- Meta description removed
- Canonical changed
- H1 changed on protected templates
- Large title churn outside release windows

### Link-graph changes
- Priority page loses unique inlinks
- Category or hub page gains crawl depth
- New redirect-chain source pages appear
- New links to broken destinations appear

### International changes
- Missing return links appear
- Non-canonical hreflang targets appear
- Noindex hreflang targets appear
- x-default disappears on a market homepage

### Performance and rendering changes
- PageSpeed opportunity counts spike on a template family
- JS-rendered metadata diverges from server HTML
- New blocked-resource issues appear

### Security and compliance changes
- CSP disappears
- HSTS disappears
- Accessibility issue counts jump
- Structured-data errors appear on monetized templates

## Delivery options
- Slack webhook
- Teams message
- Email digest
- Jira ticket
- Notion database row
- Warehouse event table

## Operational advice
- Keep the alert set small at first.
- Separate blockers from informational alerts.
- Group by template or directory, not just raw URL.
- Record baselines explicitly so the team trusts the signal.
