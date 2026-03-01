# Features Index

Last updated: 2026-01-29

This folder provides comprehensive documentation for the **screamingfrog** Python library,
which enables programmatic access to Screaming Frog SEO Spider crawl data.

---

## Document Overview

| File | Description |
|------|-------------|
| `00_index.md` | This index and navigation guide |
| `README.md` | Single comprehensive features write-up with use cases and examples |
| `01_core_api.md` | Core public API (`Crawl` class), views, and data models |
| `02_sources_and_loaders.md` | `Crawl.load()` and all source-type handling with parameters |
| `03_backends.md` | CSV, SQLite, Derby, CLI, and Hybrid backends with capabilities matrix |
| `04_tabs_and_filters.md` | Tab access, complete GUI filter list, and column metadata |
| `05_links.md` | Inlinks/outlinks traversal and link data structure |
| `06_diff.md` | Crawl-over-crawl diff with all change signals |
| `07_escape_hatches.md` | Raw table access, SQL passthrough, and advanced queries |
| `08_cli_exports.md` | CLI export helpers, profiles, and automation |
| `09_db_packaging.md` | `.dbseospider` packing/unpacking and DB-mode utilities |
| `10_config_patches.md` | ConfigBuilder patches for custom search, JS, and extractions |
| `11_schema_mapping.md` | Derby mapping system, coverage, and fallback behavior |
| `12_limits_and_gaps.md` | Known limitations, dependencies, and roadmap |
| `13_golden_path_examples.md` | Production-ready scripts for common SEO audits |

---

## Quick Navigation by Use Case

### Getting Started
- **First-time setup**: See `README.md` for installation and quick start
- **Loading crawl data**: See `02_sources_and_loaders.md` for all loader options
- **Understanding backends**: See `03_backends.md` for backend comparison

### Common Tasks
- **Find broken links**: See `13_golden_path_examples.md` → Broken Links Report
- **Audit page titles**: See `13_golden_path_examples.md` → Title/Meta Audit
- **Compare crawls**: See `06_diff.md` and `13_golden_path_examples.md` → Crawl Diff
- **Analyze link graph**: See `05_links.md` for inlinks/outlinks

### Advanced Usage
- **Raw SQL queries**: See `07_escape_hatches.md`
- **Custom extractions**: See `10_config_patches.md`
- **Automate exports**: See `08_cli_exports.md`
- **Package crawls**: See `09_db_packaging.md`

### Reference
- **All API methods**: See `01_core_api.md`
- **All GUI filters**: See `04_tabs_and_filters.md`
- **Derby mapping**: See `11_schema_mapping.md`
- **Limitations**: See `12_limits_and_gaps.md`

---

## Library Capabilities Summary

### Data Sources Supported
- CSV export folders from Screaming Frog GUI
- SQLite databases (`.db`, `.sqlite`)
- Apache Derby databases (`.dbseospider`, DB-mode project folders)
- `.seospider` crawl files (via CLI automation)
- DB crawl IDs (UUID references to ProjectInstanceData)

### Core Features
- Unified `Crawl.load()` with auto-detection
- Typed `InternalPage` view with filtering and counting
- Generic tab access for any CSV/mapped tab
- GUI-style filters matching Screaming Frog UI
- Inlinks/outlinks link graph (Derby backend)
- Crawl-over-crawl diff with 15+ change signals
- Raw SQL escape hatch for advanced queries

### Automation Features
- CLI export automation with kitchen-sink profile
- `.dbseospider` pack/unpack utilities
- `.seospider` → `.dbseospider` conversion
- ConfigBuilder patches for custom crawl settings

### Backend Comparison

| Feature | CSV | SQLite | Derby | Hybrid |
|---------|-----|--------|-------|--------|
| Internal view | ✓ | ✓ | ✓ | ✓ |
| Generic tabs | ✓ | Limited | ✓ | ✓ |
| GUI filters | ✓ | Limited | ✓ | ✓ |
| Inlinks/Outlinks | ✗ | ✗ | ✓ | ✓ |
| Raw SQL | ✗ | ✓ | ✓ | ✓ |
| Aggregations | ✗ | ✓ | ✓ | ✓ |
| Speed | Slow | Fast | Fast | Fast |

---

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `SCREAMINGFROG_CLI` | Path to Screaming Frog CLI executable |
| `SCREAMINGFROG_PROJECT_DIR` | ProjectInstanceData root directory |
| `SCREAMINGFROG_MAPPING` | Custom Derby column mapping JSON |
| `DERBY_JAR` | Derby jar path(s) for Java bridge |
| `JAVA_HOME` | Java runtime location |

---

## Version History

- **Alpha (current)**: Full Derby backend, Hybrid fallback, crawl diff, CLI automation
- **Phase 1**: Backend interface, CSV/SQLite backends, unified loader
