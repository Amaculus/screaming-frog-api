from __future__ import annotations

import json
from pathlib import Path


def test_all_inlinks_mapping_targets_links_table() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    entries = mapping.get("all_inlinks.csv", [])
    assert entries, "all_inlinks.csv mapping is missing"
    assert {entry.get("db_table") for entry in entries} == {"APP.LINKS"}
    columns = {entry.get("csv_column") for entry in entries}
    required = {
        "Type",
        "Source",
        "Destination",
        "Anchor",
        "Rel",
        "Follow",
        "Status Code",
        "Status",
        "Link Path",
        "Link Position",
        "hreflang",
        "Indexability",
        "Indexability Status",
    }
    assert required.issubset(columns)


def test_all_outlinks_mapping_targets_links_table() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    entries = mapping.get("all_outlinks.csv", [])
    assert entries, "all_outlinks.csv mapping is missing"
    assert {entry.get("db_table") for entry in entries} == {"APP.LINKS"}
