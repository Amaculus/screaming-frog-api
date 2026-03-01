from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from screamingfrog.backends.hybrid_backend import _mapping_missing_columns


@dataclass
class DummyPrimary:
    _mapping: dict[str, list[dict[str, str]]]


def _write_schema(schema_dir: Path, filename: str, columns: list[str]) -> None:
    payload = {"columns": [{"name": name} for name in columns]}
    (schema_dir / filename).write_text(json.dumps(payload), encoding="utf-8")


def test_mapping_missing_columns_uses_base_mapping_for_gui(tmp_path: Path) -> None:
    _write_schema(tmp_path, "page_titles_missing.json", ["Address", "Title 1"])
    _write_schema(tmp_path, "page_titles_all.json", ["Address", "Title 1"])
    mapping = {
        "page_titles_all.csv": [
            {"csv_column": "Address"},
            {"csv_column": "Title 1"},
        ]
    }
    primary = DummyPrimary(mapping)
    assert _mapping_missing_columns(primary, "Page Titles", "Missing", tmp_path) is False


def test_mapping_missing_columns_returns_true_when_base_missing(tmp_path: Path) -> None:
    _write_schema(tmp_path, "page_titles_missing.json", ["Address", "Title 1"])
    mapping = {
        "page_titles_all.csv": [
            {"csv_column": "Address"},
        ]
    }
    primary = DummyPrimary(mapping)
    assert _mapping_missing_columns(primary, "Page Titles", "Missing", tmp_path) is True
