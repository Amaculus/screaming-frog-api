from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.filters.names import make_tab_filename, normalize_name
from screamingfrog.models import InternalPage, Link


_INTERNAL_FILE_CANDIDATES = [
    "internal_all.csv",
    "Internal All.csv",
    "internal.csv",
]

_INTERNAL_FILTER_MAP = {
    "address": "Address",
    "status_code": "Status Code",
}
_EXPECTED_INTERNAL_COLUMNS = {"Address", "Status Code"}


class CSVBackend(CrawlBackend):
    """Backend that reads from CSV export files."""

    def __init__(self, export_dir: str):
        self.export_dir = Path(export_dir)
        if not self.export_dir.exists():
            raise FileNotFoundError(f"Export directory not found: {self.export_dir}")

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        csv_path = self._resolve_internal_file()
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            if not fieldnames:
                raise ValueError(f"CSV file {csv_path.name} has no header row")
            _validate_csv_headers(csv_path, fieldnames, _EXPECTED_INTERNAL_COLUMNS)
            for row in reader:
                if not _row_matches(row, filters, _INTERNAL_FILTER_MAP):
                    continue
                yield InternalPage.from_csv_row(row)

    def get_inlinks(self, url: str) -> Iterator[Link]:
        raise NotImplementedError("Inlinks not implemented for CSV backend yet")

    def get_outlinks(self, url: str) -> Iterator[Link]:
        raise NotImplementedError("Outlinks not implemented for CSV backend yet")

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        if table != "internal":
            raise NotImplementedError("CSV backend only supports 'internal' in Phase 1")
        return sum(1 for _ in self.get_internal(filters=filters))

    def aggregate(self, table: str, column: str, func: str) -> Any:
        if table != "internal":
            raise NotImplementedError("CSV backend only supports 'internal' in Phase 1")
        if func.lower() == "count":
            return self.count(table)
        raise NotImplementedError("CSV backend supports only count aggregation in Phase 1")

    def list_tabs(self) -> list[str]:
        return sorted({path.name for path in self.export_dir.glob("*.csv")})

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.pop("__gui__", None)
        if gui_filter:
            csv_path = self._resolve_gui_tab(tab_name, gui_filter)
        else:
            csv_path = self._resolve_tab_file(tab_name)
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            header_map = _build_header_map(reader.fieldnames or [])
            for row in reader:
                if not _row_matches(row, filters, header_map):
                    continue
                yield row

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        raise NotImplementedError("Raw access is only available for database backends.")

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        raise NotImplementedError("SQL access is only available for database backends.")

    def _resolve_internal_file(self) -> Path:
        for candidate in _INTERNAL_FILE_CANDIDATES:
            path = self.export_dir / candidate
            if path.exists():
                return path
        # fallback: search by lowercase match
        for path in self.export_dir.glob("*.csv"):
            if path.name.lower() in {c.lower() for c in _INTERNAL_FILE_CANDIDATES}:
                return path
        raise FileNotFoundError("Internal CSV export not found in export directory")

    def _resolve_tab_file(self, tab_name: str) -> Path:
        name = tab_name.strip()
        if not name:
            raise ValueError("tab_name cannot be empty")
        candidates: list[str] = []
        if not name.lower().endswith(".csv"):
            candidates.append(f"{name}.csv")
        candidates.append(name)

        normalized = normalize_name(name)
        if normalized and not normalized.lower().endswith(".csv"):
            normalized = f"{normalized}.csv"
        if normalized:
            candidates.append(normalized)

        extra: list[str] = []
        for candidate in candidates:
            lower = candidate.lower()
            if lower.endswith("_all.csv"):
                continue
            if lower.endswith(".csv"):
                extra.append(candidate[:-4] + "_all.csv")
        candidates.extend(extra)

        for candidate in candidates:
            path = self.export_dir / candidate
            if path.exists():
                return path

        for path in self.export_dir.glob("*.csv"):
            for candidate in candidates:
                if path.name.lower() == candidate.lower():
                    return path

        raise FileNotFoundError(f"CSV export not found: {name}")

    def _resolve_gui_tab(self, tab_name: str, gui_filter: Any) -> Path:
        if isinstance(gui_filter, (list, tuple, set)):
            if len(gui_filter) != 1:
                raise ValueError("CSV backend supports only a single gui filter")
            gui_filter = list(gui_filter)[0]
        filename = make_tab_filename(tab_name, str(gui_filter))
        path = self.export_dir / filename
        if path.exists():
            return path
        # fallback: try case-insensitive match
        for candidate in self.export_dir.glob("*.csv"):
            if candidate.name.lower() == filename.lower():
                return candidate
        raise FileNotFoundError(
            f"CSV export for gui filter not found: {filename}. "
            "Ensure the export profile includes this tab."
        )


def _row_matches(
    row: dict[str, Any],
    filters: Optional[dict[str, Any]],
    column_map: dict[str, str],
) -> bool:
    if not filters:
        return True
    for key, expected in filters.items():
        lookup = _normalize_key(str(key))
        column = column_map.get(lookup, key)
        actual = row.get(column)
        if isinstance(expected, (list, tuple, set)):
            if actual not in expected:
                return False
        elif callable(expected):
            if not expected(actual):
                return False
        else:
            if expected is None:
                if actual not in (None, ""):
                    return False
            elif str(actual) != str(expected):
                return False
    return True


def _normalize_key(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _validate_csv_headers(csv_path: Path, fieldnames: Sequence[str], expected: set[str]) -> None:
    actual = set(fieldnames or [])
    missing = expected - actual
    if missing:
        raise ValueError(
            f"CSV {csv_path.name} missing required columns: {sorted(missing)}. "
            f"Got: {sorted(actual)}."
        )


def _build_header_map(headers: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for header in headers:
        mapping[_normalize_key(header)] = header
    return mapping
