from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from screamingfrog.filters.names import normalize_name

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.db.connection import connect
from screamingfrog.models import InternalPage, Link


class DatabaseBackend(CrawlBackend):
    """Backend that queries the SQLite database directly."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")
        self.conn = connect(self.db_path)
        self._internal_columns = self._get_table_columns("internal")
        self._internal_column_map = {col.lower(): col for col in self._internal_columns}

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        for row in self.get_tab("internal_all", filters=filters):
            yield InternalPage.from_data(row, copy_data=False)

    def get_inlinks(self, url: str) -> Iterator[Link]:
        raise NotImplementedError("Inlinks not implemented for DB backend yet")

    def get_outlinks(self, url: str) -> Iterator[Link]:
        raise NotImplementedError("Outlinks not implemented for DB backend yet")

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        if table != "internal":
            raise NotImplementedError("DB backend only supports 'internal' in Phase 1")
        if filters:
            return sum(1 for _ in self.get_internal(filters=filters))
        return int(self.conn.execute("SELECT COUNT(*) FROM internal").fetchone()[0])

    def aggregate(self, table: str, column: str, func: str) -> Any:
        if table != "internal":
            raise NotImplementedError("DB backend only supports 'internal' in Phase 1")
        func = func.strip().upper()
        if func not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregation: {func}")
        sql = f"SELECT {func}({column}) FROM internal"
        return self.conn.execute(sql).fetchone()[0]

    def list_tabs(self) -> list[str]:
        return sorted(_SQLITE_TAB_SPECS.keys())

    def tab_columns(self, tab_name: str) -> list[str]:
        spec = _resolve_sqlite_tab(tab_name, None)
        if not spec:
            return []
        return [col.csv_column for col in spec.columns]

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.pop("__gui__", None)
        if gui_filter and isinstance(gui_filter, (list, tuple, set)):
            if len(gui_filter) != 1:
                raise ValueError("SQLite backend supports only a single gui filter")
            gui_filter = list(gui_filter)[0]

        spec = _resolve_sqlite_tab(tab_name, gui_filter)
        if not spec:
            raise NotImplementedError(f"Tab not supported for SQLite backend: {tab_name}")

        select_cols: list[str] = []
        for col in spec.columns:
            resolved = _resolve_column(self._internal_column_map, col.candidates)
            if resolved:
                if col.transform == "length":
                    select_cols.append(f"LENGTH({resolved}) AS \"{col.csv_column}\"")
                else:
                    select_cols.append(f"{resolved} AS \"{col.csv_column}\"")
            else:
                select_cols.append(f"NULL AS \"{col.csv_column}\"")
        sql = f"SELECT {', '.join(select_cols)} FROM internal"
        params: list[Any] = []
        where_parts: list[str] = []
        if spec.where_clause:
            where_parts.append(spec.where_clause)
            params.extend(spec.where_params)
        if spec.status_min is not None or spec.status_max is not None or spec.status_is_null:
            status_col = _resolve_column(
                self._internal_column_map, _SQLITE_COLUMN_CANDIDATES.get("Status Code", ())
            )
            if status_col:
                if spec.status_is_null:
                    where_parts.append(f"{status_col} IS NULL")
                else:
                    if spec.status_min is not None and spec.status_max is not None:
                        where_parts.append(f"{status_col} BETWEEN ? AND ?")
                        params.extend([spec.status_min, spec.status_max])
                    elif spec.status_min is not None:
                        where_parts.append(f"{status_col} >= ?")
                        params.append(spec.status_min)
                    elif spec.status_max is not None:
                        where_parts.append(f"{status_col} <= ?")
                        params.append(spec.status_max)
            else:
                where_parts.append("1=0")
        if spec.missing_candidates:
            missing_col = _resolve_column(self._internal_column_map, spec.missing_candidates)
            if missing_col:
                where_parts.append(f"{missing_col} IS NULL OR TRIM({missing_col}) = ''")
            else:
                where_parts.append("1=0")
        if filters:
            where, where_params = _build_sqlite_where(filters, self._internal_column_map)
            if where:
                where_parts.append(where)
                params.extend(where_params)
        if where_parts:
            sql = f"{sql} WHERE {' AND '.join(where_parts)}"
        cursor = self.conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description or []]
        for row in cursor.fetchall():
            yield {col: val for col, val in zip(columns, row)}

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        cursor = self.conn.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in cursor.description or []]
        for row in cursor.fetchall():
            yield {col: val for col, val in zip(columns, row)}

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        cursor = self.conn.execute(query, list(params or []))
        columns = [desc[0] for desc in cursor.description or []]
        for row in cursor.fetchall():
            yield {col: val for col, val in zip(columns, row)}

    def _get_table_columns(self, table_name: str) -> list[str]:
        cursor = self.conn.execute(f"PRAGMA table_info({table_name});")
        return [row[1] for row in cursor.fetchall()]
@dataclass(frozen=True)
class ColumnSpec:
    csv_column: str
    candidates: Sequence[str] = ()
    transform: Optional[str] = None


@dataclass(frozen=True)
class TabSpec:
    name: str
    columns: Sequence[ColumnSpec]
    where_clause: str = ""
    where_params: Sequence[Any] = ()
    missing_candidates: Sequence[str] = ()
    status_min: Optional[int] = None
    status_max: Optional[int] = None
    status_is_null: bool = False


_SQLITE_COLUMN_CANDIDATES: dict[str, Sequence[str]] = {
    "Address": ("address", "url", "Address", "URL"),
    "Status Code": ("status_code", "response_code", "Status Code"),
    "Title 1": ("title", "title_1", "Title 1", "page_title"),
    "Title": ("title", "page_title"),
    "Meta Description 1": ("meta_description", "meta_description_1", "Meta Description 1"),
    "Meta Keywords 1": ("meta_keywords", "meta_keywords_1", "Meta Keywords 1"),
    "H1-1": ("h1", "h1_1", "H1-1"),
    "H2-1": ("h2", "h2_1", "H2-1"),
    "H3-1": ("h3", "h3_1", "H3-1"),
    "Word Count": ("word_count", "Word Count"),
    "Indexability": ("indexability", "Indexability"),
    "Indexability Status": ("indexability_status", "Indexability Status"),
    "Canonical Link Element 1": ("canonical", "canonical_1", "canonical_link_element_1", "Canonical Link Element 1"),
    "Redirect URL": ("redirect_url", "redirect_uri", "redirect_destination", "Redirect URL"),
    "Redirect Type": ("redirect_type", "Redirect Type"),
    "Meta Robots 1": ("meta_robots", "meta_robots_1", "Meta Robots 1"),
    "X-Robots-Tag 1": ("x_robots_tag", "x_robots_tag_1", "X-Robots-Tag 1"),
    "Meta Refresh 1": ("meta_refresh", "meta_refresh_1", "Meta Refresh 1"),
}


def _cols(*names: str) -> list[ColumnSpec]:
    return [ColumnSpec(name, _SQLITE_COLUMN_CANDIDATES.get(name, (name,))) for name in names]


_SQLITE_TAB_SPECS: dict[str, TabSpec] = {
    "internal_all.csv": TabSpec(
        name="internal_all.csv",
        columns=[
            *_cols(
                "Address",
                "Status Code",
                "Title 1",
                "Meta Description 1",
                "H1-1",
                "H2-1",
                "H3-1",
                "Word Count",
                "Indexability",
                "Indexability Status",
                "Canonical Link Element 1",
                "Meta Robots 1",
                "X-Robots-Tag 1",
                "Redirect URL",
                "Redirect Type",
                "Meta Refresh 1",
            )
        ],
    ),
    "response_codes_internal_all.csv": TabSpec(
        name="response_codes_internal_all.csv",
        columns=[*_cols("Address", "Status Code", "Indexability", "Indexability Status", "Redirect URL", "Redirect Type")],
    ),
    "response_codes_internal_success_(2xx).csv": TabSpec(
        name="response_codes_internal_success_(2xx).csv",
        columns=[*_cols("Address", "Status Code", "Indexability", "Indexability Status", "Redirect URL", "Redirect Type")],
        status_min=200,
        status_max=299,
    ),
    "response_codes_internal_redirection_(3xx).csv": TabSpec(
        name="response_codes_internal_redirection_(3xx).csv",
        columns=[*_cols("Address", "Status Code", "Indexability", "Indexability Status", "Redirect URL", "Redirect Type")],
        status_min=300,
        status_max=399,
    ),
    "response_codes_internal_client_error_(4xx).csv": TabSpec(
        name="response_codes_internal_client_error_(4xx).csv",
        columns=[*_cols("Address", "Status Code", "Indexability", "Indexability Status", "Redirect URL", "Redirect Type")],
        status_min=400,
        status_max=499,
    ),
    "response_codes_internal_server_error_(5xx).csv": TabSpec(
        name="response_codes_internal_server_error_(5xx).csv",
        columns=[*_cols("Address", "Status Code", "Indexability", "Indexability Status", "Redirect URL", "Redirect Type")],
        status_min=500,
        status_max=599,
    ),
    "response_codes_internal_no_response.csv": TabSpec(
        name="response_codes_internal_no_response.csv",
        columns=[*_cols("Address", "Status Code", "Indexability", "Indexability Status", "Redirect URL", "Redirect Type")],
        status_is_null=True,
    ),
    "page_titles_all.csv": TabSpec(
        name="page_titles_all.csv",
        columns=[
            *_cols("Address", "Status Code", "Title 1"),
            ColumnSpec("Title 1 Length", _SQLITE_COLUMN_CANDIDATES.get("Title 1", ()), "length"),
        ],
    ),
    "page_titles_missing.csv": TabSpec(
        name="page_titles_missing.csv",
        columns=[
            *_cols("Address", "Status Code", "Title 1"),
            ColumnSpec("Title 1 Length", _SQLITE_COLUMN_CANDIDATES.get("Title 1", ()), "length"),
        ],
        missing_candidates=_SQLITE_COLUMN_CANDIDATES.get("Title 1", ()),
    ),
    "meta_description_all.csv": TabSpec(
        name="meta_description_all.csv",
        columns=[
            *_cols("Address", "Status Code", "Meta Description 1"),
            ColumnSpec(
                "Meta Description 1 Length",
                _SQLITE_COLUMN_CANDIDATES.get("Meta Description 1", ()),
                "length",
            ),
        ],
    ),
    "meta_description_missing.csv": TabSpec(
        name="meta_description_missing.csv",
        columns=[
            *_cols("Address", "Status Code", "Meta Description 1"),
            ColumnSpec(
                "Meta Description 1 Length",
                _SQLITE_COLUMN_CANDIDATES.get("Meta Description 1", ()),
                "length",
            ),
        ],
        missing_candidates=_SQLITE_COLUMN_CANDIDATES.get("Meta Description 1", ()),
    ),
}


def _resolve_sqlite_tab(tab_name: str, gui_filter: Optional[str]) -> Optional[TabSpec]:
    key = normalize_name(tab_name) if tab_name else ""
    if key and not key.endswith(".csv"):
        key = f"{key}.csv"
    if key in {"page_titles.csv", "page_titles_all.csv"}:
        key = "page_titles_all.csv"
    if key in {"meta_description.csv", "meta_descriptions.csv"}:
        key = "meta_description_all.csv"

    if gui_filter:
        gui = str(gui_filter).strip().lower()
        if key in {"page_titles_all.csv", "page_titles.csv"} and gui == "missing":
            key = "page_titles_missing.csv"
        if key in {"meta_description_all.csv", "meta_description.csv"} and gui == "missing":
            key = "meta_description_missing.csv"

    if key in _SQLITE_TAB_SPECS:
        return _SQLITE_TAB_SPECS[key]
    if key and not key.endswith("_all.csv"):
        alt = f"{key.removesuffix('.csv')}_all.csv"
        return _SQLITE_TAB_SPECS.get(alt)
    return None


def _resolve_column(column_map: dict[str, str], candidates: Sequence[str]) -> Optional[str]:
    for candidate in candidates:
        if candidate in column_map.values():
            return candidate
        lower = candidate.lower()
        if lower in column_map:
            return column_map[lower]
    return None


def _build_sqlite_where(
    filters: dict[str, Any], column_map: dict[str, str]
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    for key, expected in filters.items():
        candidates = _SQLITE_COLUMN_CANDIDATES.get(str(key), (str(key),))
        column = _resolve_column(column_map, candidates)
        if not column:
            continue
        if isinstance(expected, (list, tuple, set)):
            placeholders = ", ".join(["?"] * len(expected))
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(list(expected))
        elif expected is None:
            clauses.append(f"{column} IS NULL")
        else:
            clauses.append(f"{column} = ?")
            params.append(expected)
    return " AND ".join(clauses), params
