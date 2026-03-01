from __future__ import annotations

import json
from pathlib import Path


def _load_schema_columns(name: str) -> list[str]:
    data = json.loads(Path("schemas/csv", name).read_text(encoding="utf-8"))
    return [c["name"] for c in data.get("columns", []) if c.get("name")]


def _load_mapping_columns(key: str) -> list[str]:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    return [e.get("csv_column") for e in mapping.get(key, []) if e.get("csv_column")]


def test_client_error_inlinks_mapping_matches_schema_order() -> None:
    schema_cols = _load_schema_columns("client_error_(4xx)_inlinks.json")
    mapping_cols = _load_mapping_columns("client_error_(4xx)_inlinks.csv")
    assert mapping_cols == schema_cols


def test_blocked_by_robots_inlinks_mapping_matches_schema_order() -> None:
    schema_cols = _load_schema_columns("blocked_by_robots_txt_inlinks.json")
    mapping_cols = _load_mapping_columns("blocked_by_robots_txt_inlinks.csv")
    assert mapping_cols == schema_cols


def test_all_inlinks_mapping_matches_schema_order() -> None:
    schema_cols = _load_schema_columns("all_inlinks.json")
    mapping_cols = _load_mapping_columns("all_inlinks.csv")
    assert mapping_cols == schema_cols
