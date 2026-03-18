#!/usr/bin/env python3
"""
Suggest Derby mappings for sf-alpha GUI tab columns.

Follows Antonio's workflow:
- Compare CSV schema (schemas/csv/) with Derby schema (schemas/db/tables/)
- Prefer db_column, then db_expression, then NULL
- Output suggestions for mapping.json or backlog docs

Usage (run from sf-alpha repo root):
  python scripts/suggest_mappings.py --tab hreflang_all.csv
  python scripts/suggest_mappings.py --tab-family hreflang
  python scripts/suggest_mappings.py --list-unmapped
  python scripts/suggest_mappings.py --report-nulls   # regenerate mapping_nulls.md content
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


# CSV columns that must use db_expression (not raw db_column) for CSV parity — do not replace NULL with db_column.
_EXPRESSION_ONLY_CSV_COLUMNS = frozenset({
    "follow", "type", "source", "destination", "rel",  # link tabs: expression or subquery
    "indexability", "indexability status",  # CASE expression
})

# Derby columns that are blobs, backend-special, or otherwise not directly usable as a
# per-column mapping.  Never suggest these via cross-tab hints or heuristic matching.
_DENIED_DB_COLUMNS = frozenset({
    "SERIALISED_STRUCTURED_DATA",
    "ORIGINAL_CONTENT",
    "RENDERED_CONTENT",
    "RESPONSE_HEADERS",
    "LOADED_AS_A_RESOURCE",
})


def load_db_schemas(schemas_dir: Path) -> dict[str, set[str]]:
    """Load all APP.*.json from schemas/db/tables/ -> { "APP.URLS": {"ENCODED_URL", ...}, ... }."""
    tables_dir = schemas_dir / "db" / "tables"
    out: dict[str, set[str]] = {}
    if not tables_dir.exists():
        return out
    for path in tables_dir.glob("APP.*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            cols = data.get("columns") or []
            name = path.stem  # e.g. APP.URLS
            out[name] = {c.get("name") for c in cols if c.get("name")}
        except Exception:
            continue
    return out


def load_csv_schemas(schemas_dir: Path) -> dict[str, list[str]]:
    """Discover CSV schemas: key = file from JSON (e.g. internal_all.csv), value = list of column names."""
    csv_dir = schemas_dir / "csv"
    out: dict[str, list[str]] = {}
    if not csv_dir.exists():
        return out
    for path in csv_dir.glob("*.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            key = data.get("file", "").strip()
            if not key or not key.endswith(".csv"):
                continue
            cols = [c.get("name") for c in (data.get("columns") or []) if c.get("name")]
            out[key] = cols
        except Exception:
            continue
    return out


def load_mapping(mapping_path: Path) -> dict[str, list[dict]]:
    """Load schemas/mapping.json."""
    if not mapping_path.exists():
        return {}
    return json.loads(mapping_path.read_text(encoding="utf-8"))


def build_global_column_hints(mapping: dict[str, list[dict]]) -> dict[str, list[tuple[str, str]]]:
    """From existing mapping, build csv_column -> [(db_table, db_column), ...] for suggestions."""
    hints: dict[str, list[tuple[str, str]]] = {}
    for _tab, entries in mapping.items():
        for e in entries:
            csv_col = (e.get("csv_column") or "").strip()
            if not csv_col:
                continue
            key = _normalize_key(csv_col)
            table = (e.get("db_table") or "").strip()
            if e.get("db_column"):
                col = (e.get("db_column") or "").strip()
                if table and col and col not in _DENIED_DB_COLUMNS:
                    hints.setdefault(key, []).append((table, col))
            elif e.get("db_expression") and "NULL" not in str(e.get("db_expression", "")):
                # leave as needing manual expression
                continue
    return hints


def primary_table_for_tab(mapping: dict, tab_key: str) -> str | None:
    """Most common db_table in this tab's entries (for heuristic fallback)."""
    entries = mapping.get(tab_key) or []
    counts: dict[str, int] = {}
    for e in entries:
        t = (e.get("db_table") or "").strip()
        if t:
            counts[t] = counts.get(t, 0) + 1
    return max(counts, key=counts.get) if counts else None


def csv_column_to_derby_heuristic(csv_name: str, db_columns: set[str]) -> tuple[str | None, str]:
    """Suggest Derby column from CSV header using simple heuristics.

    Returns (db_column_or_None, source) where source is one of:
      "known"     — from the explicit known dict (safe for auto-apply)
      "heuristic" — uppercase or normalized match (needs manual review)
      ""          — no match found
    """
    key = _normalize_key(csv_name)
    known: dict[str, str] = {
        "address": "ENCODED_URL",
        "url": "ENCODED_URL",
        "source_page": "ENCODED_URL",
        "status_code": "RESPONSE_CODE",
        "content_type": "CONTENT_TYPE",
        "indexability": None,  # expression
        "indexability_status": None,
        "status": "RESPONSE_MSG",
        "title_1": "TITLE_1",
        "meta_description_1": "META_DESCRIPTION_1",
        "h1_1": "H1_1",
        "h2_1": "H2_1",
        "word_count": "WORD_COUNT",
        "page_size": "PAGE_SIZE",
        "size_bytes": "PAGE_SIZE",
        "crawl_depth": "CRAWL_DEPTH",
        "link_text": "LINK_TEXT",
        "anchor_text": "LINK_TEXT",
        "alt_text": "ALT_TEXT",
        "type": None,  # link type is expression
        "source": None,
        "destination": None,
    }
    if key in known:
        cand = known[key]
        if cand and cand in db_columns and cand not in _DENIED_DB_COLUMNS:
            return cand, "known"
        return cand, "known"  # might be None for expression
    # Try uppercase with underscores
    cand = key.upper().replace(" ", "_").replace("-", "_")
    if cand in db_columns and cand not in _DENIED_DB_COLUMNS:
        return cand, "heuristic"
    # Try exact normalized match only (no substring matching)
    for db_col in sorted(db_columns):
        if db_col in _DENIED_DB_COLUMNS:
            continue
        if _normalize_key(db_col) == key:
            return db_col, "heuristic"
    return None, ""


def suggest_for_tab(
    tab_key: str,
    csv_schemas: dict[str, list[str]],
    db_schemas: dict[str, set[str]],
    mapping: dict[str, list[dict]],
    global_hints: dict[str, list[tuple[str, str]]],
) -> list[dict]:
    """For one tab, return list of { csv_column, suggestion_type, db_table?, db_column? }."""
    csv_cols = csv_schemas.get(tab_key)
    if not csv_cols:
        return []
    entries_by_csv: dict[str, dict] = {}
    for e in mapping.get(tab_key) or []:
        c = (e.get("csv_column") or "").strip()
        if c:
            entries_by_csv[c] = e

    primary_table = primary_table_for_tab(mapping, tab_key)
    all_db_cols: set[str] = set()
    for cols in db_schemas.values():
        all_db_cols |= cols
    if primary_table:
        primary_cols = db_schemas.get(primary_table) or set()
    else:
        primary_cols = all_db_cols

    out: list[dict] = []
    for csv_col in csv_cols:
        key = _normalize_key(csv_col)
        existing = entries_by_csv.get(csv_col)
        if existing:
            expr = str(existing.get("db_expression") or "").strip()
            has_real = existing.get("db_column") or existing.get("header_extract") or (
                existing.get("db_expression") and expr != "NULL"
            )
            if has_real:
                out.append({"csv_column": csv_col, "status": "mapped", "existing": True})
                continue
            # mapped to NULL: still try to suggest db_column if we find one (for --apply to replace)
            if csv_col.strip().lower() in _EXPRESSION_ONLY_CSV_COLUMNS:
                out.append({"csv_column": csv_col, "status": "null", "suggestion": "NULL"})
                continue
            # Try known-dict heuristic first (safe for auto-apply)
            primary = primary_table or "APP.URLS"
            primary_cols = primary_cols or db_schemas.get(primary) or all_db_cols
            heuristic, h_source = csv_column_to_derby_heuristic(csv_col, primary_cols)
            if heuristic and (heuristic in (db_schemas.get(primary) or set())):
                note = "known mapping" if h_source == "known" else "heuristic match"
                out.append({
                    "csv_column": csv_col,
                    "status": "suggest_db_column",
                    "db_table": primary,
                    "db_column": heuristic,
                    "note": note,
                })
                continue
            # Then try cross-tab hints (not safe for auto-apply)
            hint_list = global_hints.get(key)
            if hint_list:
                table, col = hint_list[0]
                if col in (db_schemas.get(table) or set()):
                    out.append({
                        "csv_column": csv_col,
                        "status": "suggest_db_column",
                        "db_table": table,
                        "db_column": col,
                        "note": "cross-tab hint",
                    })
                    continue
            out.append({"csv_column": csv_col, "status": "null", "suggestion": "NULL"})
            continue

        # Not in mapping: suggest
        # Try known-dict heuristic first (safe for auto-apply)
        primary = primary_table or "APP.URLS"
        primary_cols = primary_cols or db_schemas.get(primary) or all_db_cols
        heuristic, h_source = csv_column_to_derby_heuristic(csv_col, primary_cols)
        if heuristic and (heuristic in (db_schemas.get(primary) or set())):
            note = "known mapping" if h_source == "known" else "heuristic match"
            out.append({
                "csv_column": csv_col,
                "status": "suggest_db_column",
                "db_table": primary,
                "db_column": heuristic,
                "note": note,
            })
            continue

        # Then try cross-tab hints (not safe for auto-apply)
        hint_list = global_hints.get(key)
        if hint_list:
            table, col = hint_list[0]
            if col in (db_schemas.get(table) or set()):
                out.append({
                    "csv_column": csv_col,
                    "status": "suggest_db_column",
                    "db_table": table,
                    "db_column": col,
                    "note": "cross-tab hint",
                })
                continue

        out.append({"csv_column": csv_col, "status": "suggest_null", "suggestion": "NULL"})
    return out


def tab_keys_matching(csv_schemas: dict[str, list[str]], tab_family: str | None, tab_exact: str | None):
    """Resolve list of tab keys from --tab-family or --tab."""
    keys = sorted(csv_schemas.keys())
    if tab_exact:
        norm = tab_exact.strip().lower()
        if not norm.endswith(".csv"):
            norm += ".csv"
        # match by key
        for k in keys:
            if k.lower() == norm or _normalize_key(k) == _normalize_key(norm):
                return [k]
        # try prefix
        return [k for k in keys if _normalize_key(k).startswith(_normalize_key(tab_exact))]
    if tab_family:
        prefix = _normalize_key(tab_family)
        return [k for k in keys if _normalize_key(k).startswith(prefix) or prefix in _normalize_key(k)]
    return keys


def _is_safe_db_column_suggestion(s: dict) -> bool:
    """Only auto-apply db_column when it comes from the explicit known dict."""
    return (s.get("note") or "") == "known mapping"


def apply_suggestions_to_mapping(
    mapping: dict[str, list[dict]],
    all_suggestions: dict[str, list[dict]],
    tab_keys: list[str],
    primary_tables: dict[str, str],
    dry_run: bool,
    safe_only: bool = True,
    null_only: bool = False,
) -> tuple[dict[str, list[dict]], list[str]]:
    """
    Merge suggestions into mapping. Returns (updated_mapping, list of change descriptions).
    If dry_run, mapping is copied and modified; otherwise mapping is mutated in place (caller should save).
    If safe_only: only apply suggest_db_column when note is "known mapping" (explicit known dict);
    cross-tab hints and heuristic matches require --apply-all.
    If null_only: only add missing columns as NULL (no db_column replacements).
    """
    if dry_run:
        import copy
        mapping = copy.deepcopy(mapping)
    changes: list[str] = []

    for tab_key in tab_keys:
        if tab_key not in mapping:
            continue
        entries = mapping[tab_key]
        primary_table = primary_tables.get(tab_key) or "APP.URLS"
        # index by csv_column for replace
        by_csv: dict[str, int] = {}
        for i, e in enumerate(entries):
            c = (e.get("csv_column") or "").strip()
            if c:
                by_csv[c] = i

        for s in all_suggestions.get(tab_key) or []:
            status = s.get("status")
            csv_col = (s.get("csv_column") or "").strip()
            if not csv_col:
                continue
            idx = by_csv.get(csv_col)

            if status == "suggest_db_column":
                if null_only:
                    continue
                if safe_only and not _is_safe_db_column_suggestion(s):
                    continue
                table = s.get("db_table") or primary_table
                col = s.get("db_column")
                if not table or not col:
                    continue
                new_entry = {"csv_column": csv_col, "db_column": col, "db_table": table}
                if idx is not None:
                    existing = entries[idx]
                    if existing.get("db_expression") and "NULL" in str(existing.get("db_expression", "")):
                        entries[idx] = new_entry
                        changes.append(f"{tab_key}: {csv_col} NULL → db_column {table}.{col}")
                    elif not existing.get("db_column") and not existing.get("db_expression"):
                        entries[idx] = new_entry
                        changes.append(f"{tab_key}: {csv_col} → db_column {table}.{col}")
                    # else already has real mapping, skip
                else:
                    entries.append(new_entry)
                    changes.append(f"{tab_key}: +{csv_col} → db_column {table}.{col}")

            elif status in ("suggest_null", "null"):
                if idx is not None:
                    continue  # already have an entry (NULL or other)
                new_entry = {"csv_column": csv_col, "db_expression": "NULL", "db_table": primary_table}
                entries.append(new_entry)
                changes.append(f"{tab_key}: +{csv_col} → NULL")

    return mapping, changes


def _existing_mapping_type(entry: dict) -> str:
    """Return a short label for what the mapping has: db_column, db_expression, or NULL."""
    if entry.get("db_column"):
        return f"db_column:{entry.get('db_table','')}.{entry.get('db_column','')}"
    if entry.get("db_expression"):
        ex = str(entry.get("db_expression", "")).strip()
        if ex == "NULL":
            return "NULL"
        return "db_expression"
    if entry.get("header_extract"):
        return "header_extract"
    return "NULL"


def validate_against_mapping(
    mapping: dict[str, list[dict]],
    all_suggestions: dict[str, list[dict]],
    tab_keys: list[str],
) -> list[dict]:
    """Compare script suggestions to existing mapping. Returns list of {tab, csv_column, existing, script_says, match}."""
    rows: list[dict] = []
    for tab_key in tab_keys:
        entries = mapping.get(tab_key) or []
        existing_by_csv = {(e.get("csv_column") or "").strip(): e for e in entries if (e.get("csv_column") or "").strip()}
        for s in all_suggestions.get(tab_key) or []:
            csv_col = (s.get("csv_column") or "").strip()
            if not csv_col:
                continue
            existing = existing_by_csv.get(csv_col)
            existing_type = _existing_mapping_type(existing) if existing else "(missing)"
            status = s.get("status", "")
            if status == "mapped":
                script_says = "agree (mapped)"
                match = True
            elif status == "suggest_db_column":
                script_says = f"db_column:{s.get('db_table','')}.{s.get('db_column','')}"
                if existing:
                    match = (
                        existing.get("db_column") == s.get("db_column")
                        and existing.get("db_table") == s.get("db_table")
                    )
                else:
                    match = False
            elif status in ("suggest_null", "null"):
                script_says = "NULL"
                match = existing_type == "NULL"
            else:
                script_says = status
                match = False
            rows.append({
                "tab": tab_key,
                "csv_column": csv_col,
                "existing": existing_type,
                "script_says": script_says,
                "match": match,
            })
    return rows


def generate_mapping_nulls_content(mapping: dict[str, list[dict]], csv_schemas: dict[str, list[str]]) -> str:
    """Generate markdown lines for mapping_nulls.md: tabs and their NULL columns."""
    lines = [
        "# Mapping NULL Columns",
        "",
        f"Updated mappings: {len(mapping)}",
        "",
        "Columns currently mapped to NULL in Derby:",
        "",
    ]
    for tab_key in sorted(mapping.keys()):
        entries = mapping.get(tab_key) or []
        null_cols = []
        for e in entries:
            if str(e.get("db_expression", "")).strip().upper() == "NULL":
                csv_col = (e.get("csv_column") or "").strip()
                if csv_col:
                    null_cols.append(csv_col)
        if null_cols:
            lines.append(f"- {tab_key}: {', '.join(null_cols)}")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Suggest Derby mappings for sf-alpha tab columns")
    ap.add_argument("--schemas", type=Path, default=None, help="Path to schemas dir (default: repo/schemas)")
    ap.add_argument("--tab", "-t", type=str, default=None, help="Single tab key, e.g. hreflang_all or hreflang_all.csv")
    ap.add_argument("--tab-family", type=str, default=None, help="Tab family prefix, e.g. hreflang → all hreflang_*.csv")
    ap.add_argument("--list-unmapped", action="store_true", help="List tabs that have unmapped columns (from CSV vs mapping)")
    ap.add_argument("--validate", action="store_true", help="Compare script suggestions to existing mapping; use with --tab/--tab-family")
    ap.add_argument("--report-nulls", action="store_true", help="Print mapping_nulls.md style report from current mapping")
    ap.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    ap.add_argument("--patch", action="store_true", help="Output mapping.json fragment (suggested db_column entries only) for manual merge")
    ap.add_argument("--apply", action="store_true", help="Merge suggested db_column and NULL entries into schemas/mapping.json")
    ap.add_argument("--apply-all", action="store_true", help="With --apply: also apply cross-tab hints and heuristic matches (default: only known-dict mappings + new NULLs)")
    ap.add_argument("--apply-null-only", action="store_true", help="With --apply: only add missing columns as NULL (no db_column changes)")
    ap.add_argument("--dry-run", action="store_true", help="With --apply: show what would be changed, do not write")
    ap.add_argument("--out", type=Path, default=None, help="Write suggestions to file (default: stdout)")
    args = ap.parse_args()

    root = _repo_root()
    schemas_dir = args.schemas or root / "schemas"
    mapping_path = schemas_dir / "mapping.json"

    db_schemas = load_db_schemas(schemas_dir)
    csv_schemas = load_csv_schemas(schemas_dir)
    mapping = load_mapping(mapping_path)
    global_hints = build_global_column_hints(mapping)

    if args.report_nulls:
        content = generate_mapping_nulls_content(mapping, csv_schemas)
        if args.out:
            args.out.write_text(content, encoding="utf-8")
        else:
            print(content)
        return

    if args.list_unmapped:
        unmapped: list[tuple[str, int]] = []
        for tab_key in sorted(csv_schemas.keys()):
            suggestions = suggest_for_tab(tab_key, csv_schemas, db_schemas, mapping, global_hints)
            need = [s for s in suggestions if s.get("status") not in ("mapped",) and "suggest" in str(s.get("status", ""))]
            if need:
                unmapped.append((tab_key, len(need)))
        if args.json:
            print(json.dumps(unmapped, indent=2))
        else:
            for tab_key, count in unmapped:
                print(f"{tab_key}\t{count} unmapped")
        return

    tab_keys = tab_keys_matching(csv_schemas, args.tab_family, args.tab)
    if not tab_keys:
        print("No matching tabs found.", file=__import__("sys").stderr)
        return

    all_suggestions: dict[str, list[dict]] = {}
    primary_tables: dict[str, str] = {}
    for tab_key in tab_keys:
        all_suggestions[tab_key] = suggest_for_tab(tab_key, csv_schemas, db_schemas, mapping, global_hints)
        pt = primary_table_for_tab(mapping, tab_key)
        if pt:
            primary_tables[tab_key] = pt

    if args.validate:
        rows = validate_against_mapping(mapping, all_suggestions, tab_keys)
        matches = sum(1 for r in rows if r["match"])
        total = len(rows)
        sys = __import__("sys")
        print(f"Validation: {matches}/{total} columns match existing mapping", file=sys.stderr)
        print("")
        # Show first 50 rows as table; then summary of mismatches
        col_w = 28
        for r in rows[:50]:
            m = "✓" if r["match"] else "✗"
            ex = (r["existing"][:col_w] + "…") if len(r["existing"]) > col_w else r["existing"]
            say = (r["script_says"][:col_w] + "…") if len(r["script_says"]) > col_w else r["script_says"]
            print(f"  {m}  {r['csv_column'][:24]:24}  existing: {ex:30}  script: {say}")
        if total > 50:
            print(f"  ... and {total - 50} more")
        mismatches = [r for r in rows if not r["match"]]
        if mismatches:
            print("", file=sys.stderr)
            print("Mismatches (script would differ from existing):", file=sys.stderr)
            for r in mismatches[:20]:
                print(f"  {r['tab']}: {r['csv_column']}  existing={r['existing']}  script={r['script_says']}", file=sys.stderr)
            if len(mismatches) > 20:
                print(f"  ... and {len(mismatches) - 20} more", file=sys.stderr)
        return

    if args.apply:
        mapping, changes = apply_suggestions_to_mapping(
            mapping, all_suggestions, tab_keys, primary_tables,
            dry_run=args.dry_run,
            safe_only=not args.apply_all,
            null_only=args.apply_null_only,
        )
        sys = __import__("sys")
        for c in changes:
            print(c, file=sys.stderr)
        if not args.dry_run and changes:
            mapping_path.write_text(json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Updated {mapping_path} ({len(changes)} change(s))", file=sys.stderr)
        elif args.dry_run and changes:
            print(f"[dry-run] Would apply {len(changes)} change(s)", file=sys.stderr)
        elif not changes:
            print("No changes to apply.", file=sys.stderr)
        return

    if args.patch:
        # Output mapping.json fragment: { "tab.csv": [ { "csv_column", "db_column", "db_table" }, ... ], ... }
        patch: dict[str, list[dict]] = {}
        for tab_key in tab_keys:
            entries = []
            for s in all_suggestions.get(tab_key) or []:
                if s.get("status") == "suggest_db_column" and s.get("db_table") and s.get("db_column"):
                    entries.append({
                        "csv_column": s["csv_column"],
                        "db_column": s["db_column"],
                        "db_table": s["db_table"],
                    })
            if entries:
                patch[tab_key] = entries
        out_text = json.dumps(patch, indent=2)
    elif args.json:
        out_text = json.dumps(all_suggestions, indent=2)
    else:
        lines = []
        for tab_key in tab_keys:
            lines.append(f"## {tab_key}")
            lines.append("")
            for s in all_suggestions.get(tab_key) or []:
                status = s.get("status", "")
                csv_col = s.get("csv_column", "")
                if status == "mapped":
                    lines.append(f"- {csv_col}: (already mapped)")
                elif status == "suggest_db_column":
                    lines.append(f"- {csv_col}: db_column → {s.get('db_table')}.{s.get('db_column')}  [{s.get('note', '')}]")
                elif status in ("suggest_null", "null"):
                    lines.append(f"- {csv_col}: NULL")
                else:
                    lines.append(f"- {csv_col}: {status}")
            lines.append("")
        out_text = "\n".join(lines)

    if args.out:
        args.out.write_text(out_text, encoding="utf-8")
    else:
        print(out_text)


if __name__ == "__main__":
    main()
