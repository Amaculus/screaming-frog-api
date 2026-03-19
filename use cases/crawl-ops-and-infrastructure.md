# Crawl Ops And Infrastructure

## Crawl Fleet Management
- Treat crawls as scheduled artifacts rather than one-off analyst events.
- Package DB-backed crawls into shared storage for downstream jobs.
- Separate crawl execution from crawl analysis so teams can reuse one crawl many times.
- Standardize naming, retention, and metadata across crawl archives.

## Shared Analysis Environments
- Load `.dbseospider` files in notebooks, scripts, CI jobs, and agent environments.
- Avoid GUI dependency for downstream analysis workers.
- Re-run audits in cloud or collaboration environments by unpacking crawl artifacts only.
- Build a common crawl registry for engineering, SEO, and data teams.

## Pipeline Reliability
- Add smoke tests that verify expected tables and minimum row counts.
- Detect corrupted or incomplete crawl artifacts before they reach reporting jobs.
- Cache derived exports or summary tables only after the source crawl passes validation.
- Build automatic retries or fallback rules around crawl ingestion.

## Operational Efficiency
- Keep Derby as the source of truth while Python handles orchestration and reporting.
- Use raw SQL escape hatches when mappings lag behind analysis needs.
- Reduce repeated crawl costs by reusing one stored artifact for multiple workflows.
- Prepare for optional analytics acceleration layers later without changing the intake contract.
