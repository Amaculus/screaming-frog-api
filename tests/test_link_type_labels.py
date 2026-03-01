from __future__ import annotations

import json
from pathlib import Path

from screamingfrog.backends.derby_backend import _link_type_name


def test_link_type_name_mapping() -> None:
    assert _link_type_name(1) == "Hyperlink"
    assert _link_type_name(6) == "Canonical"
    assert _link_type_name(8) == "Rel Prev"
    assert _link_type_name(10) == "Rel Next"
    assert _link_type_name(12) == "Hreflang (HTTP)"
    assert _link_type_name(13) == "Hreflang"
    assert _link_type_name(999) is None


def test_link_type_expression_contains_known_codes() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    expr = next(
        entry.get("db_expression")
        for entry in mapping.get("all_inlinks.csv", [])
        if entry.get("csv_column") == "Type"
    )
    for code in (1, 6, 8, 10, 12, 13):
        assert f"LINK_TYPE = {code}" in expr
