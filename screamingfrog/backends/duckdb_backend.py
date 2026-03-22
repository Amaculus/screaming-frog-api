from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.db.duckdb import (
    iter_cursor_rows,
    iter_relation_rows,
    list_exported_tabs,
    resolve_relation_name,
)
from screamingfrog.filters.names import make_tab_filename, normalize_name
from screamingfrog.models import InternalPage, Link


class DuckDBBackend(CrawlBackend):
    """Backend that reads exported crawl data from a DuckDB analytics cache."""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"DuckDB database not found: {self.db_path}")
        self._duckdb = _import_duckdb()
        self.conn = self._duckdb.connect(str(self.db_path), read_only=True)
        internal_relation = _resolve_tab_relation(self.conn, "internal_all", None)
        if not internal_relation:
            raise ValueError(
                "DuckDB cache is missing the materialized internal_all tab. Re-export the crawl."
            )
        self._internal_relation = internal_relation
        self._internal_columns = self._get_relation_columns(self._internal_relation)
        self._internal_column_map = {col.lower(): col for col in self._internal_columns}

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        for row in self._iter_relation(self._internal_relation, filters=filters):
            yield InternalPage.from_data(row, copy_data=False)

    def get_inlinks(self, url: str) -> Iterator[Link]:
        relation = resolve_relation_name(self.conn, "tab", "all_inlinks")
        if not relation:
            raise NotImplementedError("DuckDB cache does not include all_inlinks.")
        for row in self._iter_relation(relation, filters={"Address": url}):
            data = dict(row)
            data.setdefault("Destination", row.get("Destination") or row.get("Address"))
            yield Link.from_row(data)

    def get_outlinks(self, url: str) -> Iterator[Link]:
        relation = resolve_relation_name(self.conn, "tab", "all_outlinks")
        if not relation:
            raise NotImplementedError("DuckDB cache does not include all_outlinks.")
        for row in self._iter_relation(relation, filters={"Source": url}):
            yield Link.from_row(row)

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        if table != "internal":
            raise NotImplementedError("DuckDB backend currently supports count() for internal only")
        sql, params, post_filters = _build_relation_query(
            self._internal_relation,
            self._internal_columns,
            filters,
        )
        if post_filters:
            return sum(1 for _ in self.get_internal(filters=filters))
        row = self.conn.execute(f"SELECT COUNT(*) FROM ({sql}) AS sf_count", params).fetchone()
        return int(row[0]) if row else 0

    def aggregate(self, table: str, column: str, func: str) -> Any:
        if table != "internal":
            raise NotImplementedError("DuckDB backend currently supports aggregate() for internal only")
        func_name = func.strip().upper()
        if func_name not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregation: {func}")
        cursor = self.conn.execute(
            f"SELECT {func_name}({_quote_identifier(column)}) FROM {self._internal_relation}"
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def list_tabs(self) -> list[str]:
        return list_exported_tabs(self.conn)

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.pop("__gui__", None)
        relation = _resolve_tab_relation(self.conn, tab_name, gui_filter)
        if not relation:
            if gui_filter:
                raise NotImplementedError(
                    f"Tab not available in DuckDB cache: {make_tab_filename(str(tab_name), str(gui_filter))}"
                )
            raise NotImplementedError(f"Tab not available in DuckDB cache: {tab_name}")
        return self._iter_relation(relation, filters=filters)

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        relation = resolve_relation_name(self.conn, "raw", table)
        if not relation:
            raise NotImplementedError(f"Raw table not available in DuckDB cache: {table}")
        return iter_relation_rows(self.conn, relation)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        cursor = self.conn.execute(query, list(params or []))
        columns = [desc[0] for desc in cursor.description or []]
        for row in iter_cursor_rows(cursor):
            yield {col: val for col, val in zip(columns, row)}

    def tab_columns(self, tab_name: str) -> list[str]:
        relation = _resolve_tab_relation(self.conn, tab_name, None)
        if not relation:
            return []
        return self._get_relation_columns(relation)

    def _iter_relation(
        self,
        relation_name: str,
        filters: Optional[dict[str, Any]] = None,
    ) -> Iterator[dict[str, Any]]:
        relation_columns = self._get_relation_columns(relation_name)
        sql, params, post_filters = _build_relation_query(relation_name, relation_columns, filters)
        cursor = self.conn.execute(sql, params)
        columns = [desc[0] for desc in cursor.description or relation_columns]
        for row in iter_cursor_rows(cursor):
            record = {col: val for col, val in zip(columns, row)}
            if post_filters and not _row_matches(record, post_filters):
                continue
            yield record

    def _get_relation_columns(self, relation_name: str) -> list[str]:
        schema_name, _, table_name = relation_name.partition(".")
        if not table_name:
            schema_name, table_name = "main", schema_name
        cursor = self.conn.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE lower(table_schema) = lower(?)
              AND lower(table_name) = lower(?)
            ORDER BY ordinal_position
            """,
            [schema_name, table_name],
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _build_relation_query(
    relation_name: str,
    columns: Sequence[str],
    filters: Optional[dict[str, Any]],
) -> tuple[str, list[Any], dict[str, Any]]:
    sql = f"SELECT * FROM {relation_name}"
    if not filters:
        return sql, [], {}

    column_map = {_normalize_key(column): column for column in columns}
    where_parts: list[str] = []
    params: list[Any] = []
    post_filters: dict[str, Any] = {}
    for key, expected in dict(filters).items():
        if str(key).startswith("__"):
            continue
        column = column_map.get(_normalize_key(str(key)))
        if not column or callable(expected):
            post_filters[_normalize_key(str(key))] = expected
            continue
        quoted = _quote_identifier(column)
        if isinstance(expected, (list, tuple, set)):
            values = list(expected)
            if not values:
                where_parts.append("1=0")
                continue
            where_parts.append(f"{quoted} IN ({', '.join('?' for _ in values)})")
            params.extend(values)
            continue
        if expected is None:
            where_parts.append(f"({quoted} IS NULL OR TRIM(CAST({quoted} AS VARCHAR)) = '')")
            continue
        where_parts.append(f"{quoted} = ?")
        params.append(expected)
    if where_parts:
        sql = f"{sql} WHERE {' AND '.join(where_parts)}"
    return sql, params, post_filters


def _row_matches(row: dict[str, Any], filters: dict[str, Any]) -> bool:
    lookup = {_normalize_key(key): value for key, value in row.items()}
    for key, expected in filters.items():
        actual = lookup.get(key)
        if callable(expected):
            if not expected(actual):
                return False
            continue
        if expected is None:
            if actual not in (None, ""):
                return False
            continue
        if isinstance(expected, (list, tuple, set)):
            if actual not in expected:
                return False
            continue
        if str(actual) != str(expected):
            return False
    return True


def _normalize_key(value: str) -> str:
    return str(value).strip().lower().replace(" ", "_")


def _resolve_tab_relation(conn: Any, tab_name: str, gui_filter: Any) -> str | None:
    candidates = _tab_export_candidates(tab_name, gui_filter)
    for candidate in candidates:
        relation = resolve_relation_name(conn, "tab", candidate)
        if relation:
            return relation
    return None


def _tab_export_candidates(tab_name: str, gui_filter: Any) -> list[str]:
    if isinstance(gui_filter, (list, tuple, set)):
        if len(gui_filter) != 1:
            raise ValueError("DuckDB backend supports only a single gui filter")
        gui_filter = list(gui_filter)[0]

    name = str(tab_name).strip()
    if not name:
        raise ValueError("tab_name cannot be empty")

    if gui_filter:
        return [make_tab_filename(name, str(gui_filter))]

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
        if lower.endswith("_all.csv") or not lower.endswith(".csv"):
            continue
        extra.append(candidate[:-4] + "_all.csv")
    candidates.extend(extra)

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        lowered = candidate.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        ordered.append(candidate)
    return ordered


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _import_duckdb():
    try:
        import duckdb
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency error path
        raise ImportError("duckdb is required for DuckDB backend support.") from exc
    return duckdb
