from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Iterator, Optional, Sequence

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.db.duckdb import (
    _helper_relation_name,
    _relation_exists,
    _write_relation,
    export_duckdb_from_backend,
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
        self._lazy_source_backend: Any | None = None
        self._lazy_source_backend_factory: Callable[[], Any] | None = None
        self._lazy_source_label: str | None = None
        self._available_tabs: tuple[str, ...] | None = None
        self._open_connection()
        internal_relation = _resolve_tab_relation(self.conn, "internal_all", None)
        self._internal_relation = internal_relation
        self._internal_columns = self._get_relation_columns(self._internal_relation) if internal_relation else []
        self._internal_column_map = {col.lower(): col for col in self._internal_columns}

    def configure_lazy_source(
        self,
        source_backend: Any | None = None,
        *,
        source_backend_factory: Callable[[], Any] | None = None,
        source_label: str | None = None,
        available_tabs: Sequence[str] | None = None,
    ) -> None:
        self._lazy_source_backend = source_backend
        self._lazy_source_backend_factory = source_backend_factory
        self._lazy_source_label = source_label
        if available_tabs is not None:
            self._available_tabs = tuple(str(name) for name in available_tabs if str(name).strip())

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        if not self._internal_relation:
            source_backend = self.get_lazy_source_backend()
            if source_backend is not None:
                yield from source_backend.get_internal(filters=filters)
                return
            if not self.ensure_internal():
                raise NotImplementedError(
                    "DuckDB cache does not include internal_all and no lazy source is configured."
                )
        for row in self._iter_relation(self._internal_relation, filters=filters):
            yield InternalPage.from_data(row, copy_data=False)

    def get_inlinks(self, url: str) -> Iterator[Link]:
        relation = self.ensure_helper_relation("links_core")
        if relation:
            for row in self._iter_relation(relation, filters={"Destination": url}):
                yield Link.from_row(row)
            return
        source_backend = self.get_lazy_source_backend()
        if source_backend is not None:
            yield from source_backend.get_inlinks(url)
            return
        relation = resolve_relation_name(self.conn, "tab", "all_inlinks")
        if not relation and self.ensure_tab("all_inlinks"):
            relation = resolve_relation_name(self.conn, "tab", "all_inlinks")
        if relation:
            for row in self._iter_relation(relation, filters={"Address": url}):
                data = dict(row)
                data.setdefault("Destination", row.get("Destination") or row.get("Address"))
                yield Link.from_row(data)
            return
        for row in self._iter_raw_links("in", url):
            yield Link.from_row(row)

    def get_outlinks(self, url: str) -> Iterator[Link]:
        relation = self.ensure_helper_relation("links_core")
        if relation:
            for row in self._iter_relation(relation, filters={"Source": url}):
                yield Link.from_row(row)
            return
        source_backend = self.get_lazy_source_backend()
        if source_backend is not None:
            yield from source_backend.get_outlinks(url)
            return
        relation = resolve_relation_name(self.conn, "tab", "all_outlinks")
        if not relation and self.ensure_tab("all_outlinks"):
            relation = resolve_relation_name(self.conn, "tab", "all_outlinks")
        if relation:
            for row in self._iter_relation(relation, filters={"Source": url}):
                yield Link.from_row(row)
            return
        for row in self._iter_raw_links("out", url):
            yield Link.from_row(row)

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        if table != "internal":
            raise NotImplementedError("DuckDB backend currently supports count() for internal only")
        if not self._internal_relation:
            source_backend = self.get_lazy_source_backend()
            if source_backend is not None:
                return source_backend.count(table, filters=filters)
        basic_relation = self._basic_internal_relation(filters)
        if basic_relation:
            basic_columns = self._get_relation_columns(basic_relation)
            sql, params, post_filters = _build_relation_query(
                basic_relation,
                basic_columns,
                filters,
            )
            if post_filters:
                return sum(1 for _ in self.get_internal(filters=filters))
            row = self.conn.execute(f"SELECT COUNT(*) FROM ({sql}) AS sf_count", params).fetchone()
            return int(row[0]) if row else 0
        if not self.ensure_internal():
            raise NotImplementedError(
                "DuckDB cache does not include internal_all and no lazy source is configured."
            )
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
        if not self._internal_relation:
            source_backend = self.get_lazy_source_backend()
            if source_backend is not None:
                return source_backend.aggregate(table, column, func)
        basic_relation = self._basic_internal_relation_for_column(column)
        if basic_relation:
            func_name = func.strip().upper()
            if func_name not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
                raise ValueError(f"Unsupported aggregation: {func}")
            cursor = self.conn.execute(
                f"SELECT {func_name}({_quote_identifier(column)}) FROM {basic_relation}"
            )
            row = cursor.fetchone()
            return row[0] if row else None
        if not self.ensure_internal():
            raise NotImplementedError(
                "DuckDB cache does not include internal_all and no lazy source is configured."
            )
        func_name = func.strip().upper()
        if func_name not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregation: {func}")
        cursor = self.conn.execute(
            f"SELECT {func_name}({_quote_identifier(column)}) FROM {self._internal_relation}"
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def list_tabs(self) -> list[str]:
        exported = list_exported_tabs(self.conn)
        if not self._available_tabs:
            return exported
        seen: set[str] = set()
        ordered: list[str] = []
        for name in [*exported, *self._available_tabs]:
            normalized = str(name).strip()
            if not normalized:
                continue
            lowered = normalized.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            ordered.append(normalized)
        return ordered

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.pop("__gui__", None)
        relation = _resolve_tab_relation(self.conn, tab_name, gui_filter)
        if not relation:
            source_backend = self.get_lazy_source_backend()
            if source_backend is not None:
                source_filters = dict(filters)
                if gui_filter is not None:
                    source_filters["__gui__"] = gui_filter
                try:
                    return source_backend.get_tab(tab_name, filters=source_filters)
                except (AttributeError, NotImplementedError, ValueError):
                    pass
            self.ensure_tab(tab_name, gui_filter=gui_filter)
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
            self.ensure_raw_tables((table,))
            relation = resolve_relation_name(self.conn, "raw", table)
        if not relation:
            raise NotImplementedError(f"Raw table not available in DuckDB cache: {table}")
        return iter_relation_rows(self.conn, relation)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        self._ensure_sql_relations(query)
        cursor = self.conn.execute(query, list(params or []))
        columns = [desc[0] for desc in cursor.description or []]
        for row in iter_cursor_rows(cursor):
            yield {col: val for col, val in zip(columns, row)}

    def tab_columns(self, tab_name: str) -> list[str]:
        relation = _resolve_tab_relation(self.conn, tab_name, None)
        if not relation:
            source_backend = self.get_lazy_source_backend()
            if source_backend is not None and hasattr(source_backend, "tab_columns"):
                try:
                    return list(source_backend.tab_columns(tab_name))
                except (AttributeError, NotImplementedError, ValueError):
                    pass
            self.ensure_tab(tab_name)
            relation = _resolve_tab_relation(self.conn, tab_name, None)
        if not relation:
            return []
        return self._get_relation_columns(relation)

    def ensure_internal(self) -> bool:
        if self._internal_relation:
            return True
        if not self.ensure_tab("internal_all"):
            return False
        relation = _resolve_tab_relation(self.conn, "internal_all", None)
        if not relation:
            return False
        self._internal_relation = relation
        self._internal_columns = self._get_relation_columns(relation)
        self._internal_column_map = {col.lower(): col for col in self._internal_columns}
        return True

    def ensure_helper_relation(self, helper_name: str) -> str | None:
        relation = _helper_relation_name(helper_name)
        if _relation_exists(self.conn, relation):
            return relation
        rows = self._helper_rows(helper_name)
        if rows is None:
            return None
        self.conn.close()
        try:
            conn = self._duckdb.connect(str(self.db_path))
            try:
                if not _write_relation(conn, relation, rows):
                    return None
            finally:
                conn.close()
        finally:
            self._open_connection()
        return relation if _relation_exists(self.conn, relation) else None

    def ensure_raw_tables(self, tables: Sequence[str]) -> bool:
        requested = tuple(str(table).strip().upper() for table in tables if str(table).strip())
        if not requested:
            return True
        missing = [name for name in requested if not resolve_relation_name(self.conn, "raw", name)]
        if not missing:
            return True
        return self._materialize_exports(tables=missing)

    def ensure_tab(self, tab_name: str, *, gui_filter: Any = None) -> bool:
        candidates = _tab_export_candidates(tab_name, gui_filter)
        if any(resolve_relation_name(self.conn, "tab", candidate) for candidate in candidates):
            return True
        for candidate in candidates:
            if self._materialize_exports(tabs=(candidate,)):
                if resolve_relation_name(self.conn, "tab", candidate):
                    return True
        return False

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

    def _iter_raw_links(self, direction: str, url: str) -> Iterator[dict[str, Any]]:
        self.ensure_raw_tables(("APP.URLS", "APP.LINKS", "APP.UNIQUE_URLS"))
        urls_relation = resolve_relation_name(self.conn, "raw", "APP.URLS")
        links_relation = resolve_relation_name(self.conn, "raw", "APP.LINKS")
        unique_urls_relation = resolve_relation_name(self.conn, "raw", "APP.UNIQUE_URLS")
        if not urls_relation or not links_relation or not unique_urls_relation:
            raise NotImplementedError("DuckDB cache does not include the raw link relations.")

        if direction == "in":
            where_clause = "d.ENCODED_URL = ?"
            select_address = "d.ENCODED_URL AS destination_url"
        else:
            where_clause = "s.ENCODED_URL = ?"
            select_address = "d.ENCODED_URL AS destination_url"
        sql = f"""
            SELECT
                s.ENCODED_URL AS source_url,
                {select_address},
                l.ALT_TEXT AS alt_text,
                l.LINK_TEXT AS anchor_text,
                u.RESPONSE_CODE AS destination_status_code,
                u.RESPONSE_MSG AS destination_status,
                l.NOFOLLOW AS nofollow,
                l.UGC AS ugc,
                l.SPONSORED AS sponsored,
                l.NOOPENER AS noopener,
                l.NOREFERRER AS noreferrer,
                l.TARGET AS target_value,
                l.PATH_TYPE AS path_type,
                l.ELEMENT_PATH AS element_path,
                l.ELEMENT_POSITION AS element_position,
                l.HREF_LANG AS href_lang,
                l.LINK_TYPE AS link_type,
                l.SCOPE AS scope_value,
                l.ORIGIN AS origin_value
            FROM {links_relation} l
            JOIN {unique_urls_relation} s ON l.SRC_ID = s.ID
            JOIN {unique_urls_relation} d ON l.DST_ID = d.ID
            LEFT JOIN {urls_relation} u ON u.ENCODED_URL = d.ENCODED_URL
            WHERE {where_clause}
            ORDER BY s.ENCODED_URL, d.ENCODED_URL, l.LINK_TEXT
        """
        cursor = self.conn.execute(sql, [url])
        columns = [desc[0] for desc in cursor.description or []]
        for row in iter_cursor_rows(cursor):
            yield _shape_raw_link_row({col: val for col, val in zip(columns, row)})

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

    def _basic_internal_relation(self, filters: Optional[dict[str, Any]]) -> str | None:
        if self._internal_relation:
            return None
        filter_keys = {
            _normalize_key(str(key))
            for key in dict(filters or {}).keys()
            if not str(key).startswith("__")
        }
        if filter_keys - {"address", "status_code"}:
            return None
        return self.ensure_helper_relation("internal_basic")

    def _basic_internal_relation_for_column(self, column: str) -> str | None:
        if self._internal_relation:
            return None
        if _normalize_key(column) not in {"address", "status_code"}:
            return None
        return self.ensure_helper_relation("internal_basic")

    def _helper_rows(self, helper_name: str) -> Iterator[dict[str, Any]] | None:
        source_backend = self.get_lazy_source_backend()
        if source_backend is None:
            return None
        normalized = str(helper_name).strip().lower()
        if normalized == "internal_basic":
            return _iter_internal_basic_rows_from_source(source_backend)
        if normalized == "links_core":
            return _iter_links_core_rows_from_source(source_backend)
        return None

    def _materialize_exports(
        self,
        *,
        tables: Sequence[str] = (),
        tabs: Sequence[str] = (),
    ) -> bool:
        source_backend = self.get_lazy_source_backend()
        if source_backend is None:
            return False
        requested_tables = tuple(str(name).strip().upper() for name in tables if str(name).strip())
        requested_tabs = tuple(str(name).strip() for name in tabs if str(name).strip())
        if not requested_tables and not requested_tabs:
            return True
        self.conn.close()
        try:
            for tab_name in requested_tabs or ():
                try:
                    export_duckdb_from_backend(
                        source_backend,
                        self.db_path,
                        tables=(),
                        tabs=(tab_name,),
                        if_exists="auto",
                        source_label=self._lazy_source_label,
                    )
                except (FileNotFoundError, NotImplementedError, ValueError):
                    continue
            if requested_tables:
                export_duckdb_from_backend(
                    source_backend,
                    self.db_path,
                    tables=requested_tables,
                    tabs=(),
                    if_exists="auto",
                    source_label=self._lazy_source_label,
                )
        finally:
            self._open_connection()
        return True

    def get_lazy_source_backend(self) -> Any | None:
        if self._lazy_source_backend is not None:
            return self._lazy_source_backend
        if self._lazy_source_backend_factory is None:
            return None
        self._lazy_source_backend = self._lazy_source_backend_factory()
        return self._lazy_source_backend

    def _open_connection(self) -> None:
        self.conn = self._duckdb.connect(str(self.db_path), read_only=True)

    def _ensure_sql_relations(self, query: str) -> None:
        raw_tables: set[str] = set()
        for schema_name, table_name in re.findall(
            r"(?is)\b(?:from|join)\s+([A-Za-z_][\w$]*)\s*\.\s*([A-Za-z_][\w$]*)",
            str(query),
        ):
            if str(schema_name).strip().upper() != "APP":
                continue
            raw_tables.add(f"APP.{str(table_name).strip().upper()}")
        if raw_tables:
            self.ensure_raw_tables(sorted(raw_tables))


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


def _shape_raw_link_row(row: dict[str, Any]) -> dict[str, Any]:
    nofollow = _to_bool(row.get("nofollow"))
    ugc = _to_bool(row.get("ugc"))
    sponsored = _to_bool(row.get("sponsored"))
    noopener = _to_bool(row.get("noopener"))
    noreferrer = _to_bool(row.get("noreferrer"))
    follow = None if nofollow is None else not nofollow
    destination = row.get("destination_url")
    return {
        "Type": _link_type_name(row.get("link_type")),
        "Source": row.get("source_url"),
        "Address": destination,
        "Destination": destination,
        "Alt Text": row.get("alt_text"),
        "Anchor": row.get("anchor_text"),
        "Status Code": row.get("destination_status_code"),
        "Status": row.get("destination_status"),
        "Follow": follow,
        "Target": row.get("target_value"),
        "Rel": _rel_value(nofollow, ugc, sponsored, noopener, noreferrer),
        "Path Type": row.get("path_type"),
        "Link Path": row.get("element_path"),
        "Link Position": row.get("element_position"),
        "hreflang": row.get("href_lang"),
        "Link Type": row.get("link_type"),
        "Scope": row.get("scope_value"),
        "Origin": row.get("origin_value"),
        "NoFollow": nofollow,
        "UGC": ugc,
        "Sponsored": sponsored,
        "Noopener": noopener,
        "Noreferrer": noreferrer,
    }


def _iter_internal_basic_rows_from_source(source_backend: Any) -> Iterator[dict[str, Any]]:
    sql = """
        SELECT
            ENCODED_URL AS "Address",
            RESPONSE_CODE AS "Status Code"
        FROM APP.URLS
        WHERE IS_INTERNAL = TRUE
        ORDER BY ENCODED_URL
    """
    if hasattr(source_backend, "sql"):
        try:
            for row in source_backend.sql(sql):
                yield {
                    "Address": row.get("Address") or row.get("address") or row.get("ENCODED_URL"),
                    "Status Code": row.get("Status Code")
                    if "Status Code" in row
                    else row.get("status_code", row.get("RESPONSE_CODE")),
                }
            return
        except Exception:
            pass
    if not hasattr(source_backend, "raw"):
        return
    for row in source_backend.raw("APP.URLS"):
        address = row.get("ENCODED_URL") or row.get("Address")
        if not address:
            continue
        yield {
            "Address": address,
            "Status Code": row.get("RESPONSE_CODE", row.get("Status Code")),
        }


def _iter_links_core_rows_from_source(source_backend: Any) -> Iterator[dict[str, Any]]:
    sql = """
        SELECT
            s.ENCODED_URL AS source_url,
            d.ENCODED_URL AS destination_url,
            l.ALT_TEXT AS alt_text,
            l.LINK_TEXT AS anchor_text,
            u.RESPONSE_CODE AS destination_status_code,
            u.RESPONSE_MSG AS destination_status,
            l.NOFOLLOW AS nofollow,
            l.UGC AS ugc,
            l.SPONSORED AS sponsored,
            l.NOOPENER AS noopener,
            l.NOREFERRER AS noreferrer,
            l.TARGET AS target_value,
            l.PATH_TYPE AS path_type,
            l.ELEMENT_PATH AS element_path,
            l.ELEMENT_POSITION AS element_position,
            l.HREF_LANG AS href_lang,
            l.LINK_TYPE AS link_type,
            l.SCOPE AS scope_value,
            l.ORIGIN AS origin_value
        FROM APP.LINKS l
        JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID
        JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID
        LEFT JOIN APP.URLS u ON u.ENCODED_URL = d.ENCODED_URL
    """
    if hasattr(source_backend, "sql"):
        try:
            for row in source_backend.sql(sql):
                yield _shape_raw_link_row(dict(row))
            return
        except Exception:
            pass
    if not hasattr(source_backend, "raw"):
        return
    urls = {
        str(row.get("ENCODED_URL")): row for row in source_backend.raw("APP.URLS") if row.get("ENCODED_URL")
    }
    unique_urls: dict[Any, str] = {}
    for row in source_backend.raw("APP.UNIQUE_URLS"):
        if row.get("ID") is None or row.get("ENCODED_URL") is None:
            continue
        unique_urls[row.get("ID")] = str(row.get("ENCODED_URL"))
    for row in source_backend.raw("APP.LINKS"):
        source_url = unique_urls.get(row.get("SRC_ID"))
        destination_url = unique_urls.get(row.get("DST_ID"))
        if not source_url or not destination_url:
            continue
        destination_data = urls.get(destination_url, {})
        yield _shape_raw_link_row(
            {
                "source_url": source_url,
                "destination_url": destination_url,
                "alt_text": row.get("ALT_TEXT"),
                "anchor_text": row.get("LINK_TEXT"),
                "destination_status_code": destination_data.get("RESPONSE_CODE"),
                "destination_status": destination_data.get("RESPONSE_MSG"),
                "nofollow": row.get("NOFOLLOW"),
                "ugc": row.get("UGC"),
                "sponsored": row.get("SPONSORED"),
                "noopener": row.get("NOOPENER"),
                "noreferrer": row.get("NOREFERRER"),
                "target_value": row.get("TARGET"),
                "path_type": row.get("PATH_TYPE"),
                "element_path": row.get("ELEMENT_PATH"),
                "element_position": row.get("ELEMENT_POSITION"),
                "href_lang": row.get("HREF_LANG"),
                "link_type": row.get("LINK_TYPE"),
                "scope_value": row.get("SCOPE"),
                "origin_value": row.get("ORIGIN"),
            }
        )


def _rel_value(
    nofollow: bool | None,
    ugc: bool | None,
    sponsored: bool | None,
    noopener: bool | None,
    noreferrer: bool | None,
) -> str | None:
    rel_tokens: list[str] = []
    if nofollow:
        rel_tokens.append("nofollow")
    if ugc:
        rel_tokens.append("ugc")
    if sponsored:
        rel_tokens.append("sponsored")
    if noopener:
        rel_tokens.append("noopener")
    if noreferrer:
        rel_tokens.append("noreferrer")
    if not rel_tokens:
        return None
    return " ".join(rel_tokens)


def _link_type_name(value: Any) -> str | None:
    code = _to_int(value)
    if code is None:
        return None if value in (None, "") else str(value)
    return {
        1: "Hyperlink",
        6: "Canonical",
        8: "Rel Prev",
        10: "Rel Next",
        12: "Hreflang (HTTP)",
        13: "Hreflang",
    }.get(code, str(code))


def _to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


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
