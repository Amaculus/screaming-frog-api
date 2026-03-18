# sf-alpha scripts

## suggest_mappings.py

Helps contributors map Screaming Frog GUI tab columns to the Derby backend by comparing CSV export schemas with the Derby schema and existing `schemas/mapping.json`.

**Run from the sf-alpha repo root** (where `schemas/` and `screamingfrog/` live).

### Usage

| Command | Description |
|--------|-------------|
| `python scripts/suggest_mappings.py --tab hreflang_all.csv` | Suggestions for a single tab (`.csv` optional) |
| `python scripts/suggest_mappings.py --tab-family hreflang` | All tabs whose key starts with the family (e.g. `hreflang_*`) |
| `python scripts/suggest_mappings.py --list-unmapped` | List tabs that have columns not yet mapped (or mapped to NULL) |
| `python scripts/suggest_mappings.py --report-nulls` | Print content for `schemas/mapping_nulls.md` from current mapping |
| `python scripts/suggest_mappings.py --patch --tab my_tab` | Output a JSON fragment of suggested `db_column` entries to merge into `mapping.json` |
| `python scripts/suggest_mappings.py --apply --dry-run --tab X` | Show what would be merged into `mapping.json` (no write) |
| `python scripts/suggest_mappings.py --apply --tab X` | Merge suggestions into `schemas/mapping.json` (safe only: known-dict mappings + new NULLs) |
| `python scripts/suggest_mappings.py --apply --apply-all --tab-family X` | Also apply cross-tab hints and heuristic matches (review with `--dry-run` first) |
| `python scripts/suggest_mappings.py --apply --apply-null-only` | Only add missing columns as NULL |
| `python scripts/suggest_mappings.py --json` | Machine-readable suggestion payload |

### Workflow (with a coding agent)

1. Pick a tab family (e.g. **hreflang**, **structured_data** — see Antonio’s backlog).
2. Run `--tab-family <family>` to see current status and suggestions.
3. For each suggested `db_column`: confirm against `schemas/db/tables/*.json`; add to `schemas/mapping.json`.
4. For columns with no Derby equivalent: add `db_expression: "NULL"` (and optionally document in `schemas/mapping_nulls.md`).
5. Run tests (e.g. `pytest tests/test_mapping_*.py`).

### Options

- `--schemas PATH` — Override schemas directory (default: repo `schemas/`).
- `--out PATH` — Write output to file instead of stdout.

**Apply behaviour & safety tiers:**

| Tier | Note in output | Auto-applied by `--apply`? | Example |
|------|---------------|---------------------------|---------|
| **known mapping** | `known mapping` | Yes | `Address` → `ENCODED_URL`, `Status Code` → `RESPONSE_CODE` |
| **cross-tab hint** | `cross-tab hint` | No (needs `--apply-all`) | Same CSV column mapped in another tab |
| **heuristic match** | `heuristic match` | No (needs `--apply-all`) | Uppercase/normalized name matches a Derby column |

By default `--apply` only applies (1) `db_column` suggestions from the explicit **known dict** and (2) adding missing columns as **NULL**. Use `--apply-all` to also apply cross-tab hints and heuristic matches (always review with `--dry-run` first). Use `--apply-null-only` to only add missing columns as NULL.

Columns that must use expressions (e.g. Follow, Type, Source) are never auto-replaced from NULL.

**Denylist:** Certain Derby columns (`SERIALISED_STRUCTURED_DATA`, `ORIGINAL_CONTENT`, `RENDERED_CONTENT`, `RESPONSE_HEADERS`, `LOADED_AS_A_RESOURCE`) are blobs or backend-special and are never suggested via cross-tab hints or heuristic matching.

### Output

- **Human-readable:** Per-tab list of columns with status: already mapped, suggested `db_column` (with table and note), or suggested NULL.
- **`--patch`:** JSON object keyed by tab filename, each value a list of `{ "csv_column", "db_column", "db_table" }` for manual merge into `mapping.json`.
- **`--report-nulls`:** Markdown for `schemas/mapping_nulls.md` listing all tabs and their NULL-mapped columns.
