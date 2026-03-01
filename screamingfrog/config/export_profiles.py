from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from typing import Iterable


@dataclass(frozen=True)
class ExportProfile:
    export_tabs: list[str]
    bulk_exports: list[str]


def get_export_profile(name: str = "kitchen_sink") -> ExportProfile:
    normalized = name.strip().lower().replace("-", "_")
    if normalized != "kitchen_sink":
        raise ValueError(f"Unknown export profile: {name}")

    export_tabs = _load_lines("exports_kitchen_sink_tabs.txt")
    bulk_exports = _load_lines("exports_kitchen_sink_bulk.txt")
    return ExportProfile(export_tabs=export_tabs, bulk_exports=bulk_exports)


def _load_lines(filename: str) -> list[str]:
    with resources.files(__package__).joinpath(filename).open("r", encoding="utf-8") as handle:
        return [line.strip() for line in handle if line.strip()]
