# Quality Assurance And Governance

## Release QA
- Run standard crawl acceptance checks on every major template release.
- Compare crawl snapshots before and after deployments.
- Maintain protected URL sets with stricter regression thresholds.
- Catch silent metadata, canonical, robots, and linking regressions before production reporting.

## SEO Engineering Governance
- Define a stable contract for what every crawl must include.
- Turn technical SEO checks into versioned, testable code rather than analyst memory.
- Build reusable validation rules around fields, tabs, and raw SQL queries.
- Track which checks are exact Derby mappings versus inferred or derived fields.

## Team Enablement
- Give analysts, engineers, and PMs the same crawl-derived source of truth.
- Package standard workflows into scripts, CLIs, MCP tools, or internal libraries.
- Reduce manual QA drift across teams and client accounts.
- Preserve audit logic in code so it survives team turnover.

## Governance Reporting
- Publish issue trend lines, ownership queues, and unresolved regressions.
- Separate policy failures from one-off content mistakes.
- Create documented severity rules for technical issues.
- Build sign-off checklists for migrations, redesigns, and platform releases.
