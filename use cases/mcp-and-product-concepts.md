# MCP And Product Concepts

## Internal MCP Servers
- Wrap common crawl queries behind MCP tools for Claude Code, Codex, or internal agents.
- Expose safe parameterized actions like broken-link reports, crawl diffs, or section audits.
- Keep raw SQL available for advanced users while giving agents narrower high-trust actions.
- Package crawl loading, querying, and reporting as one composable tool surface.

## Agent Skills
- Build skills that take a crawl artifact and emit a fixed technical audit.
- Create specialized skills for migrations, hreflang, internal linking, or content QA.
- Standardize prompts around stable typed methods instead of GUI instructions.
- Attach evidence rows and sample URLs directly to agent outputs.

## Productized Interfaces
- Build lightweight analyst tools around `.dbseospider` intake plus Python reports.
- Offer opinionated commands for recurring workflows while leaving raw SQL underneath.
- Separate stable mapped APIs from faster-moving raw-access layers.
- Treat the library as the core engine under CLIs, MCPs, notebooks, and web apps.

## Packaging Strategy
- Bundle crawl execution, analysis, and reporting into one install path where practical.
- Keep Derby-first workflows independent from local GUI use after crawl creation.
- Preserve reproducible file-based artifacts so users can collaborate asynchronously.
- Use the same engine for local scripts, hosted apps, and agent-driven workflows.
