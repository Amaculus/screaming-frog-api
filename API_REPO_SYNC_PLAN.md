# API Repo -> Alpha Sync Plan

This file records what is worth carrying over from `C:\Users\Antonio\screamingfrog`
into `C:\Users\Antonio\sf-alpha`.

## Summary

- `sf-alpha` is already ahead on runtime/library code.
- `schemas/mapping.json` is effectively the same in both repos, so there is no mapping
  advantage to pull from the API repo.
- The only things worth syncing are docs, research artifacts, and optional packaging
  material.

## Copy Into Alpha

These are useful and low-risk to copy.

### Contributor / research docs

- `AGENTS.md`
- `db_vs_schema_report.md`
- `HIDDEN_DATA.md`
- `proposals.md`

Reason: these help contributors understand reverse-engineering findings, known hidden
data, and future direction. They do not risk regressing the library.

### Packaging / publishing doc

- `PYPI_README.md`

Reason: useful if `sf-alpha` becomes the package/publish source of truth.

### Reverse-engineering / discovery assets

- `schema-discovery/`
- `sample_exports/`

Reason: useful as internal contributor material for mapping work, schema validation,
and future parity checks.

Note: this is optional. If copied, it should be treated as maintainer-only/reference
material, not user-facing package content.

### Build-in-public / history docs

- `STORY.md`
- `DEVELOPMENT_STORY.md`

Reason: useful for launch/history context if you want the alpha repo to preserve the
project narrative.

## Do Not Copy Into Alpha

These would likely regress `sf-alpha` or add noise.

### Runtime code that alpha already surpasses

- `screamingfrog/__init__.py`
- `screamingfrog/backends/base.py`
- `screamingfrog/backends/cli_backend.py`
- `screamingfrog/backends/csv_backend.py`
- `screamingfrog/backends/db_backend.py`
- `screamingfrog/backends/derby_backend.py`
- `screamingfrog/backends/hybrid_backend.py`
- `screamingfrog/cli/__init__.py`
- `screamingfrog/cli/exports.py`
- `screamingfrog/models/__init__.py`
- `screamingfrog/models/internal.py`
- `screamingfrog/models/diff.py`

Reason: `sf-alpha` already has newer functionality and tests around:

- query builder
- crawl diff
- redirect/canonical chain helpers
- top-level `Crawl.raw()` / `Crawl.sql()`
- `sfconfig_adapter`
- Derby streaming/fetchmany behavior
- Derby SQL normalization fixes
- blob-based filter support

### Tests that are older than alpha's current behavior

- `tests/test_backends.py`
- `tests/test_cli_exports.py`
- `tests/test_derby_internal_mapping.py`
- `tests/test_diff.py`
- `tests/test_internal_page.py`
- `tests/test_loaders.py`

Reason: these local API repo versions are not the source of truth. `sf-alpha` already
contains broader or newer coverage.

### Junk / local-only material

Do not copy:

- `examples/carbon_snippets/`
- temporary scripts like `tmp_*.py`
- local audit/client deliverables
- local credentials/tokens
- ad hoc analysis scripts in repo root unless deliberately curated

Reason: these are local artifacts, not library assets.

## Practical Sync Order

1. Copy contributor docs (`AGENTS.md`, `HIDDEN_DATA.md`, `db_vs_schema_report.md`,
   `proposals.md`).
2. Copy `PYPI_README.md` if alpha will be the publish repo.
3. Decide whether `schema-discovery/` and `sample_exports/` belong in-repo or should
   live in a separate maintainer/reference repo.
4. Ignore runtime Python files from the API repo unless a later line-by-line review
   identifies a specific improvement worth porting.

## Bottom Line

The API repo is not ahead of alpha on library capability. The sync should be
documentation/research only, not code.
