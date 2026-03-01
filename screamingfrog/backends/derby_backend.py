from __future__ import annotations

import gzip
import json
import re
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence
from urllib.parse import urljoin

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.db.derby import (
    ensure_java_home,
    extract_dbseospider,
    find_derby_db_root,
    resolve_derby_jars,
)
from screamingfrog.filters.names import make_tab_filename, normalize_name
from screamingfrog.filters.registry import get_filter
from screamingfrog.models import InternalPage, Link


_INTERNAL_MAPPING_KEY = "internal_all.csv"
_CHAIN_TAB_KEYS = {
    "redirect_chains.csv",
    "redirect_and_canonical_chains.csv",
    "canonical_chains.csv",
    "redirects.csv",
}
_CHAIN_MAX_HOPS = 10


class DerbyBackend(CrawlBackend):
    """Backend that queries the Apache Derby database inside .dbseospider crawls."""

    def __init__(
        self,
        db_path: str,
        mapping_path: Optional[str] = None,
        derby_jar: Optional[str] = None,
        work_dir: Optional[str] = None,
    ):
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Crawl file not found: {self.db_path}")

        self._work_dir = Path(work_dir) if work_dir else None
        self._db_root = self._resolve_db_root()
        self._mapping = _load_mapping(mapping_path)
        self._table, self._column_map = _resolve_internal_mapping(self._mapping)
        self._derby_jars = resolve_derby_jars(derby_jar)
        self._conn = _connect_derby(self._db_root, self._derby_jars)
        self._internal_columns = _fetch_column_names(self._conn, self._table)
        self._internal_alias_map = _resolve_internal_alias_map(
            self._mapping, self._table, self._internal_columns
        )
        self._internal_header_extract_map = _resolve_internal_header_extract_map(
            self._mapping, self._table
        )
        self._internal_expr_selects = _resolve_internal_expression_selects(
            self._mapping, self._table
        )

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        sql = f"SELECT * FROM {self._table}"
        params: list[Any] = []
        if filters:
            where, params = _build_where(filters, self._column_map)
            sql = f"{sql} WHERE {where}"
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description or self._internal_columns]
        for row in cursor.fetchall():
            data = {col: val for col, val in zip(columns, row)}
            for alias, csv_col, _ in self._internal_expr_selects:
                if csv_col not in data and alias in data:
                    data[csv_col] = data.get(alias)
                data.pop(alias, None)
            # Expose CSV-style aliases (e.g., "Status Code") for mapped direct columns.
            for csv_col, db_col in self._internal_alias_map.items():
                if csv_col not in data and db_col in data:
                    data[csv_col] = data[db_col]
            if self._internal_header_extract_map:
                headers = _headers_from_blob(
                    data.get("HTTP_RESPONSE_HEADER_COLLECTION")
                    or data.get("http_response_header_collection")
                )
                links = _parse_link_headers(headers.get("link", [])) if headers else []
                for csv_col, extract in self._internal_header_extract_map.items():
                    if csv_col in data:
                        continue
                    data[csv_col] = _extract_header_value(extract, headers or {}, links or [])
            yield InternalPage.from_db_row(list(data.keys()), tuple(data.values()))

    def get_inlinks(self, url: str) -> Iterator[Link]:
        url_id = self._resolve_unique_url_id(url)
        if url_id is None:
            return iter(())
        sql = _LINKS_BASE_SELECT + " WHERE l.DST_ID = ?"
        return self._iter_links(sql, [url_id])

    def get_outlinks(self, url: str) -> Iterator[Link]:
        url_id = self._resolve_unique_url_id(url)
        if url_id is None:
            return iter(())
        sql = _LINKS_BASE_SELECT + " WHERE l.SRC_ID = ?"
        return self._iter_links(sql, [url_id])

    def _resolve_unique_url_id(self, url: str) -> Optional[int]:
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT ID FROM APP.UNIQUE_URLS WHERE ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY",
            [url],
        )
        row = cursor.fetchone()
        if not row:
            return None
        try:
            return int(row[0])
        except (TypeError, ValueError):
            return None

    def _iter_links(self, sql: str, params: list[Any]) -> Iterator[Link]:
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        for row in cursor.fetchall():
            yield Link.from_row(_link_row_to_dict(row))

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        if table != "internal":
            raise NotImplementedError("Derby backend only supports 'internal' in Phase 1")
        sql = f"SELECT COUNT(*) FROM {self._table}"
        params: list[Any] = []
        if filters:
            where, params = _build_where(filters, self._column_map)
            sql = f"{sql} WHERE {where}"
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return int(cursor.fetchone()[0])

    def aggregate(self, table: str, column: str, func: str) -> Any:
        if table != "internal":
            raise NotImplementedError("Derby backend only supports 'internal' in Phase 1")
        func = func.strip().upper()
        if func not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise ValueError(f"Unsupported aggregation: {func}")
        sql = f"SELECT {func}({column}) FROM {self._table}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return cursor.fetchone()[0]

    def list_tabs(self) -> list[str]:
        return sorted(_normalize_tab_name(name) for name in self._mapping.keys())

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.pop("__gui__", None)
        normalized = _normalize_tab_name(tab_name)
        if normalized in _CHAIN_TAB_KEYS:
            return self._get_chain_tab(normalized, filters)
        table, entries, gui_defs, supplementary = _resolve_tab_entries(
            self._mapping, tab_name, gui_filter
        )
        if not entries:
            raise ValueError(f"No columns mapped for tab: {tab_name}")
        select_items: list[str] = []
        csv_columns: list[str] = []
        entry_indexes: list[int | None] = []
        header_index: int | None = None
        encoded_url_index: int | None = None

        if supplementary and _table_supports_encoded_url(table):
            select_items.append("ENCODED_URL")
            encoded_url_index = len(select_items) - 1

        for entry in entries:
            if entry.get("header_extract"):
                if header_index is None:
                    select_items.append("HTTP_RESPONSE_HEADER_COLLECTION")
                    header_index = len(select_items) - 1
                entry_indexes.append(None)
                csv_columns.append(entry["csv_column"])
                continue

            expr = entry.get("db_expression")
            if expr:
                select_items.append(str(expr))
            else:
                select_items.append(entry["db_column"])
            entry_indexes.append(len(select_items) - 1)
            csv_columns.append(entry["csv_column"])
        select_cols = ", ".join(select_items)
        join_sql = ""
        where_parts: list[str] = []
        params: list[Any] = []

        join_table, join_on, join_type = _resolve_join(gui_defs)
        if join_table and join_on:
            join_sql = f" {join_type} JOIN {join_table} j ON {join_on}"

        sql = f"SELECT {select_cols} FROM {table}{join_sql}"
        params: list[Any] = []
        if filters:
            where, params = _build_where_from_entries(filters, entries)
            where_parts.append(where)

        for filt in gui_defs:
            if filt.sql_where:
                where_parts.append(filt.sql_where)

        if where_parts:
            sql = f"{sql} WHERE {' AND '.join(where_parts)}"

        supplementary_map = _build_supplementary_map(supplementary)
        supplementary_cache: dict[tuple[str, str], dict[str, Any]] = {}

        def fetch_supplementary(table_name: str, encoded_url: str) -> dict[str, Any]:
            cache_key = (table_name, encoded_url)
            if cache_key in supplementary_cache:
                return supplementary_cache[cache_key]
            spec = supplementary_map.get(table_name) or {}
            db_columns = sorted(set(str(col) for col in spec.values()))
            if not db_columns:
                supplementary_cache[cache_key] = {}
                return {}
            cursor_inner = self._conn.cursor()
            cursor_inner.execute(
                f"SELECT {', '.join(db_columns)} FROM {table_name} "
                "WHERE ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY",
                [encoded_url],
            )
            row_inner = cursor_inner.fetchone()
            if not row_inner:
                data_inner: dict[str, Any] = {}
            else:
                data_inner = {
                    column: value for column, value in zip(db_columns, row_inner)
                }
            supplementary_cache[cache_key] = data_inner
            return data_inner

        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        for row in cursor.fetchall():
            headers = None
            links = None
            if header_index is not None:
                headers = _headers_from_blob(row[header_index])
                links = _parse_link_headers(headers.get("link", [])) if headers else []
            output: dict[str, Any] = {}
            for entry, idx, column in zip(entries, entry_indexes, csv_columns):
                if entry.get("header_extract"):
                    output[column] = _extract_header_value(
                        entry["header_extract"], headers or {}, links or []
                    )
                else:
                    output[column] = row[idx] if idx is not None else None

            if supplementary_map and encoded_url_index is not None:
                encoded_url = row[encoded_url_index]
                if encoded_url:
                    encoded_text = str(encoded_url)
                    for table_name, csv_to_db in supplementary_map.items():
                        extra_data = fetch_supplementary(table_name, encoded_text)
                        for csv_col, db_col in csv_to_db.items():
                            output.setdefault(csv_col, extra_data.get(db_col))
                else:
                    for csv_to_db in supplementary_map.values():
                        for csv_col in csv_to_db:
                            output.setdefault(csv_col, None)
            yield output

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in cursor.description or []]
        for row in cursor.fetchall():
            yield {col: val for col, val in zip(columns, row)}

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        cursor = self._conn.cursor()
        cursor.execute(query, list(params or []))
        columns = [desc[0] for desc in cursor.description or []]
        for row in cursor.fetchall():
            yield {col: val for col, val in zip(columns, row)}

    def _get_chain_tab(
        self, tab_key: str, filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        entries = self._mapping.get(tab_key, [])
        if not entries:
            raise ValueError(f"No columns mapped for tab: {tab_key}")
        columns = [entry.get("csv_column") for entry in entries if entry.get("csv_column")]

        idx_expr = None
        idx_status_expr = None
        for entry in self._mapping.get(_INTERNAL_MAPPING_KEY, []):
            if entry.get("csv_column") == "Indexability":
                idx_expr = entry.get("db_expression")
            if entry.get("csv_column") == "Indexability Status":
                idx_status_expr = entry.get("db_expression")

        url_cache: dict[str, dict[str, Any]] = {}
        inlink_cache: dict[str, dict[str, Any]] = {}
        canonical_cache: dict[str, Optional[str]] = {}
        indexability_cache: dict[str, tuple[Any, Any]] = {}

        def fetch_url_details(url: str) -> Optional[dict[str, Any]]:
            if url in url_cache:
                return url_cache[url]
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT RESPONSE_CODE, RESPONSE_MSG, CONTENT_TYPE, NUM_METAREFRESH, "
                "META_FULL_URL_1, META_FULL_URL_2, HTTP_RESPONSE_HEADER_COLLECTION "
                "FROM APP.URLS WHERE ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY",
                [url],
            )
            row = cursor.fetchone()
            if not row:
                return None
            headers = _headers_from_blob(row[6])
            data = {
                "url": url,
                "response_code": row[0],
                "response_msg": row[1],
                "content_type": row[2],
                "num_metarefresh": row[3],
                "meta_url_1": row[4],
                "meta_url_2": row[5],
                "headers": headers,
            }
            url_cache[url] = data
            return data

        def fetch_inlink_details(url: str) -> dict[str, Any]:
            if url in inlink_cache:
                return inlink_cache[url]
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT s.ENCODED_URL, l.ALT_TEXT, l.LINK_TEXT, l.ELEMENT_PATH, l.ELEMENT_POSITION "
                "FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
                "WHERE d.ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY",
                [url],
            )
            row = cursor.fetchone()
            details = {
                "Source": row[0] if row else None,
                "Alt Text": row[1] if row else None,
                "Anchor Text": row[2] if row else None,
                "Link Path": row[3] if row else None,
                "Link Position": row[4] if row else None,
            }
            inlink_cache[url] = details
            return details

        def fetch_indexability(url: str) -> tuple[Any, Any]:
            if url in indexability_cache:
                return indexability_cache[url]
            if not idx_expr or not idx_status_expr:
                indexability_cache[url] = (None, None)
                return (None, None)
            cursor = self._conn.cursor()
            cursor.execute(
                f"SELECT {idx_expr}, {idx_status_expr} FROM APP.URLS "
                "WHERE ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY",
                [url],
            )
            row = cursor.fetchone()
            result = (row[0] if row else None, row[1] if row else None)
            indexability_cache[url] = result
            return result

        def fetch_canonical_target(url: str, headers: dict[str, list[str]]) -> Optional[str]:
            if url in canonical_cache:
                return canonical_cache[url]
            cursor = self._conn.cursor()
            cursor.execute(
                "SELECT d.ENCODED_URL FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
                "WHERE s.ENCODED_URL = ? AND l.LINK_TYPE = 6 FETCH FIRST 1 ROWS ONLY",
                [url],
            )
            row = cursor.fetchone()
            canonical = row[0] if row else None
            if not canonical and headers:
                links = _parse_link_headers(headers.get("link", []))
                canonical = _extract_link_rel(links, "canonical")
            canonical_cache[url] = canonical
            return canonical

        def normalize_target(base: str, target: Optional[str]) -> Optional[str]:
            if not target:
                return None
            text = str(target).strip()
            if not text:
                return None
            return urljoin(base, text)

        def resolve_location(headers: dict[str, list[str]]) -> Optional[str]:
            if not headers:
                return None
            locations = headers.get("location", [])
            if not locations:
                return None
            return locations[0]

        def build_chain(start_url: str, mode: str) -> Optional[tuple[list[dict[str, Any]], list[str], list[str], bool, bool]]:
            steps: list[dict[str, Any]] = []
            hop_types: list[str] = []
            hop_targets: list[str] = []
            visited: set[str] = set()
            loop = False
            temp_redirect = False
            current = start_url

            while len(steps) < _CHAIN_MAX_HOPS:
                if current in visited:
                    loop = True
                    break
                visited.add(current)
                data = fetch_url_details(current)
                if not data:
                    break
                steps.append(data)
                next_url = None
                hop_type = None
                if mode in {"redirect", "redirect_and_canonical"}:
                    code = data.get("response_code")
                    if code is not None and 300 <= int(code) < 400:
                        next_url = resolve_location(data.get("headers") or {})
                        if next_url:
                            hop_type = "HTTP Redirect"
                            if int(code) in {302, 303, 307}:
                                temp_redirect = True
                    if not next_url and data.get("num_metarefresh"):
                        next_url = data.get("meta_url_1") or data.get("meta_url_2")
                        if next_url:
                            hop_type = "Meta Refresh"

                if next_url:
                    next_url = normalize_target(current, next_url)
                if not next_url or next_url == current:
                    break
                hop_types.append(hop_type or "HTTP Redirect")
                hop_targets.append(next_url)
                current = next_url

            if mode in {"canonical", "redirect_and_canonical"}:
                while len(steps) < _CHAIN_MAX_HOPS:
                    data = fetch_url_details(current)
                    if not data:
                        break
                    canonical = fetch_canonical_target(current, data.get("headers") or {})
                    canonical = normalize_target(current, canonical)
                    if not canonical or canonical == current:
                        break
                    if canonical in visited:
                        loop = True
                        break
                    hop_types.append("Canonical")
                    hop_targets.append(canonical)
                    current = canonical
                    visited.add(current)
                    next_data = fetch_url_details(current)
                    if not next_data:
                        steps.append(
                            {
                                "url": current,
                                "response_code": None,
                                "response_msg": None,
                                "content_type": None,
                                "num_metarefresh": 0,
                                "meta_url_1": None,
                                "meta_url_2": None,
                                "headers": {},
                            }
                        )
                        break
                    steps.append(next_data)

                if mode == "canonical" and not any(t == "Canonical" for t in hop_types):
                    return None

            if mode == "redirect" and not hop_types:
                return None

            return steps, hop_types, hop_targets, loop, temp_redirect

        def chain_type_for(tab: str, hop_types: list[str]) -> Optional[str]:
            has_redirect = any(t in {"HTTP Redirect", "Meta Refresh"} for t in hop_types)
            has_canonical = any(t == "Canonical" for t in hop_types)
            if tab == "canonical_chains.csv":
                return "Canonical" if has_canonical else None
            if tab == "redirect_and_canonical_chains.csv":
                if has_redirect and has_canonical:
                    return "Redirect & Canonical"
                if has_canonical:
                    return "Canonical"
            if has_redirect:
                return "HTTP Redirect" if any(t == "HTTP Redirect" for t in hop_types) else "Meta Refresh"
            return None

        def hop_count_for(tab: str, hop_types: list[str]) -> int:
            if tab == "canonical_chains.csv":
                return sum(1 for t in hop_types if t == "Canonical")
            if tab == "redirect_and_canonical_chains.csv":
                return len(hop_types)
            return sum(1 for t in hop_types if t in {"HTTP Redirect", "Meta Refresh"})

        def set_if(row: dict[str, Any], key: str, value: Any) -> None:
            if key in row:
                row[key] = value

        mode = "redirect"
        if tab_key == "canonical_chains.csv":
            mode = "canonical"
        elif tab_key == "redirect_and_canonical_chains.csv":
            mode = "redirect_and_canonical"

        start_urls: list[str] = []
        norm_filters = { _normalize_key(str(k)): v for k, v in filters.items() }
        address_filter = norm_filters.get("address")
        address_values: Optional[list[Any]] = None
        if isinstance(address_filter, (list, tuple, set)):
            address_values = list(address_filter)
        elif address_filter is not None:
            address_values = [address_filter]

        cursor = self._conn.cursor()
        if mode in {"redirect", "redirect_and_canonical"}:
            sql = (
                "SELECT ENCODED_URL FROM APP.URLS "
                "WHERE (RESPONSE_CODE BETWEEN 300 AND 399 OR NUM_METAREFRESH > 0)"
            )
            params: list[Any] = []
            if address_values:
                placeholders = ", ".join(["?"] * len(address_values))
                sql += f" AND ENCODED_URL IN ({placeholders})"
                params.extend(address_values)
            cursor.execute(sql, params)
            start_urls.extend([row[0] for row in cursor.fetchall() if row and row[0]])

        if mode in {"canonical", "redirect_and_canonical"}:
            sql = (
                "SELECT DISTINCT s.ENCODED_URL FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE l.LINK_TYPE = 6"
            )
            params = []
            if address_values:
                placeholders = ", ".join(["?"] * len(address_values))
                sql += f" AND s.ENCODED_URL IN ({placeholders})"
                params.extend(address_values)
            cursor.execute(sql, params)
            start_urls.extend([row[0] for row in cursor.fetchall() if row and row[0]])

        seen: set[str] = set()
        for start_url in start_urls:
            if start_url in seen:
                continue
            seen.add(start_url)
            result = build_chain(start_url, mode)
            if not result:
                continue
            steps, hop_types, hop_targets, loop, temp_redirect = result
            chain_type = chain_type_for(tab_key, hop_types)
            hop_count = hop_count_for(tab_key, hop_types)

            row: dict[str, Any] = {col: None for col in columns}
            inlink = fetch_inlink_details(start_url)
            set_if(row, "Chain Type", chain_type)
            set_if(row, "Number of Redirects", hop_count)
            set_if(row, "Number of Redirects/Canonicals", hop_count)
            set_if(row, "Number of Canonicals", hop_count)
            set_if(row, "Loop", loop)
            set_if(row, "Temp Redirect in Chain", temp_redirect)
            set_if(row, "Source", inlink.get("Source"))
            set_if(row, "Alt Text", inlink.get("Alt Text"))
            set_if(row, "Anchor Text", inlink.get("Anchor Text"))
            set_if(row, "Link Path", inlink.get("Link Path"))
            set_if(row, "Link Position", inlink.get("Link Position"))
            set_if(row, "Address", start_url)

            final = steps[-1] if steps else None
            final_url = final.get("url") if final else None
            idx_val, idx_status_val = (None, None)
            if final_url:
                idx_val, idx_status_val = fetch_indexability(final_url)
            set_if(row, "Final Address", final_url)
            set_if(row, "Final Content", final.get("content_type") if final else None)
            set_if(row, "Final Status Code", final.get("response_code") if final else None)
            set_if(row, "Final Status", final.get("response_msg") if final else None)
            set_if(row, "Final Indexability", idx_val)
            set_if(row, "Final Indexability Status", idx_status_val)

            for i in range(1, _CHAIN_MAX_HOPS + 1):
                if i <= len(steps):
                    step = steps[i - 1]
                    set_if(row, f"Content {i}", step.get("content_type"))
                    set_if(row, f"Status Code {i}", step.get("response_code"))
                    set_if(row, f"Status {i}", step.get("response_msg"))
                if i <= len(hop_targets):
                    set_if(row, f"Redirect Type {i}", hop_types[i - 1])
                    set_if(row, f"Redirect URL {i}", hop_targets[i - 1])

            yield row

    def _resolve_db_root(self) -> Path:
        if self.db_path.is_dir():
            if (self.db_path / "service.properties").exists():
                return self.db_path
            db_root = find_derby_db_root(self.db_path)
            if db_root is None:
                raise RuntimeError(
                    "Could not locate Derby database inside directory. "
                    "Provide a .dbseospider file or a Derby database root."
                )
            return db_root

        if self.db_path.suffix.lower() != ".dbseospider":
            raise ValueError(
                "Derby backend expects a .dbseospider crawl file or Derby database directory"
            )

        if zipfile_is_zip(self.db_path):
            extract_dir = self._work_dir or Path(tempfile.mkdtemp(prefix="sf_derby_"))
            extract_dbseospider(self.db_path, extract_dir)
            db_root = find_derby_db_root(extract_dir)
            if db_root is None:
                raise RuntimeError("Could not locate Derby database inside .dbseospider")
            return db_root
        raise RuntimeError(".dbseospider file is not a valid zip archive")


def _load_mapping(mapping_path: Optional[str]) -> dict[str, Any]:
    candidates = []
    if mapping_path:
        candidates.append(Path(mapping_path))
    env_mapping = os.environ.get("SCREAMINGFROG_MAPPING")
    if env_mapping:
        candidates.append(Path(env_mapping))
    candidates.append(Path.cwd() / "schemas" / "mapping.json")
    candidates.append(Path(__file__).resolve().parents[2] / "schemas" / "mapping.json")

    for candidate in candidates:
        if candidate.exists():
            return json.loads(candidate.read_text(encoding="utf-8"))

    raise FileNotFoundError(
        "Mapping file not found. Provide mapping_path or set SCREAMINGFROG_MAPPING."
    )


def _resolve_internal_mapping(mapping: dict[str, Any]) -> tuple[str, dict[str, str]]:
    entries = mapping.get(_INTERNAL_MAPPING_KEY)
    if not entries:
        raise ValueError(f"Mapping does not contain {_INTERNAL_MAPPING_KEY}")

    table_counts: dict[str, int] = {}
    column_map: dict[str, str] = {}

    for entry in entries:
        table = entry.get("db_table")
        if not table:
            continue
        table_counts[table] = table_counts.get(table, 0) + 1
        csv_col = entry.get("csv_column", "").strip().lower()
        db_col = entry.get("db_column")
        if csv_col == "address" and db_col:
            column_map["address"] = db_col
        if csv_col == "status code" and db_col:
            column_map["status_code"] = db_col

    if not table_counts:
        raise ValueError("Mapping does not include db_table values for internal_all.csv")

    url_tables = [
        name
        for name in table_counts
        if name.upper().endswith(".URLS") or name.upper() == "URLS"
    ]
    if url_tables:
        table = sorted(
            ((name, table_counts[name]) for name in url_tables),
            key=lambda item: item[1],
            reverse=True,
        )[0][0]
    else:
        table = sorted(table_counts.items(), key=lambda item: item[1], reverse=True)[0][0]

    if "address" not in column_map:
        raise ValueError("Mapping missing Address -> db_column mapping")

    return table, column_map


def _resolve_internal_alias_map(
    mapping: dict[str, Any], table: str, columns: Sequence[str]
) -> dict[str, str]:
    entries = mapping.get(_INTERNAL_MAPPING_KEY) or []
    db_lookup = {str(col).strip().lower(): str(col) for col in columns}
    aliases: dict[str, str] = {}
    seen_csv: set[str] = set()

    for entry in entries:
        if entry.get("db_table") != table:
            continue
        if entry.get("header_extract"):
            continue
        csv_col = str(entry.get("csv_column") or "").strip()
        db_col = str(entry.get("db_column") or "").strip()
        if not csv_col or not db_col:
            continue
        csv_key = csv_col.lower()
        if csv_key in seen_csv:
            continue
        resolved_db = db_lookup.get(db_col.lower())
        if not resolved_db:
            continue
        aliases[csv_col] = resolved_db
        seen_csv.add(csv_key)

    return aliases


def _resolve_internal_header_extract_map(
    mapping: dict[str, Any], table: str
) -> dict[str, dict[str, Any]]:
    entries = mapping.get(_INTERNAL_MAPPING_KEY) or []
    extracts: dict[str, dict[str, Any]] = {}
    seen_csv: set[str] = set()

    for entry in entries:
        if entry.get("db_table") != table:
            continue
        extract = entry.get("header_extract")
        if not extract:
            continue
        csv_col = str(entry.get("csv_column") or "").strip()
        if not csv_col:
            continue
        csv_key = csv_col.lower()
        if csv_key in seen_csv:
            continue
        extracts[csv_col] = dict(extract)
        seen_csv.add(csv_key)

    return extracts


def _resolve_internal_expression_selects(
    mapping: dict[str, Any], table: str
) -> list[tuple[str, str, str]]:
    entries = mapping.get(_INTERNAL_MAPPING_KEY) or []
    selects: list[tuple[str, str, str]] = []
    seen_csv: set[str] = set()
    index = 0

    for entry in entries:
        if entry.get("db_table") != table:
            continue
        expr = entry.get("db_expression")
        if not expr:
            continue
        csv_col = str(entry.get("csv_column") or "").strip()
        if not csv_col:
            continue
        csv_key = csv_col.lower()
        if csv_key in seen_csv:
            continue
        alias = f"SF_EXPR_{index}"
        expr_str = str(expr).strip()
        if expr_str.upper() == "NULL":
            expr_str = "CAST(NULL AS VARCHAR(1))"
        selects.append((alias, csv_col, expr_str))
        seen_csv.add(csv_key)
        index += 1

    return selects


def _resolve_tab_entries(
    mapping: dict[str, Any], tab_name: str, gui_filter: Any
) -> tuple[str, list[dict[str, Any]], list[Any], list[dict[str, Any]]]:
    gui_defs = _resolve_gui_defs(tab_name, gui_filter)
    key = _normalize_tab_name(tab_name)
    entries = mapping.get(key)
    if not entries and gui_filter:
        filename = make_tab_filename(tab_name, str(_first_gui_name(gui_filter)))
        entries = mapping.get(filename)
    if not entries:
        alt = f"{_normalize_tab_name(tab_name).removesuffix('.csv')}_all.csv"
        entries = mapping.get(alt)
    if not entries:
        raise ValueError(f"Mapping does not contain tab: {tab_name}")

    table_counts: dict[str, int] = {}
    for entry in entries:
        table = entry.get("db_table")
        if not table:
            continue
        table_counts[table] = table_counts.get(table, 0) + 1

    if not table_counts:
        raise ValueError(f"Mapping does not include db_table values for {key}")

    preferred = _preferred_tables(table_counts)
    table = preferred[0]

    selected = [
        entry
        for entry in entries
        if entry.get("db_table") == table
        and (entry.get("db_column") or entry.get("db_expression"))
    ]
    selected_csv = {
        _normalize_key(str(entry.get("csv_column") or ""))
        for entry in selected
        if entry.get("csv_column")
    }
    supplementary: list[dict[str, Any]] = []
    for entry in entries:
        if entry.get("db_table") == table:
            continue
        if not entry.get("db_column"):
            continue
        if entry.get("db_expression") or entry.get("header_extract"):
            continue
        csv_column = str(entry.get("csv_column") or "").strip()
        if not csv_column:
            continue
        csv_key = _normalize_key(csv_column)
        if csv_key in selected_csv:
            continue
        if not _can_lookup_tab_table_by_encoded_url(table, str(entry.get("db_table") or "")):
            continue
        supplementary.append(entry)
        selected_csv.add(csv_key)

    return table, selected, gui_defs, supplementary


def _can_lookup_tab_table_by_encoded_url(base_table: str, other_table: str) -> bool:
    return _table_supports_encoded_url(base_table) and _table_supports_encoded_url(other_table)


def _table_supports_encoded_url(table: str) -> bool:
    upper = str(table or "").strip().upper()
    return upper in {"APP.URLS", "URLS", "APP.PAGE_SPEED_API", "APP.LANGUAGE_ERROR"}


def _build_supplementary_map(
    entries: list[dict[str, Any]]
) -> dict[str, dict[str, str]]:
    mapping: dict[str, dict[str, str]] = {}
    for entry in entries:
        table = str(entry.get("db_table") or "").strip()
        csv_col = str(entry.get("csv_column") or "").strip()
        db_col = str(entry.get("db_column") or "").strip()
        if not table or not csv_col or not db_col:
            continue
        table_map = mapping.setdefault(table, {})
        table_map.setdefault(csv_col, db_col)
    return mapping


def _build_where(filters: dict[str, Any], column_map: dict[str, str]) -> tuple[str, list[Any]]:
    clauses = []
    params: list[Any] = []
    for key, expected in filters.items():
        lookup = _normalize_key(str(key))
        column = column_map.get(lookup, key)
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


def _build_where_from_entries(
    filters: dict[str, Any], entries: list[dict[str, Any]]
) -> tuple[str, list[Any]]:
    csv_map = {
        _normalize_key(entry.get("csv_column", "")): entry.get("db_column")
        for entry in entries
        if entry.get("db_column")
    }
    column_map: dict[str, str] = {k: v for k, v in csv_map.items() if v}
    return _build_where(filters, column_map)


def _normalize_key(value: str) -> str:
    return value.strip().lower().replace(" ", "_")


def _normalize_tab_name(name: str) -> str:
    name = name.strip()
    if not name:
        return name
    normalized = normalize_name(name)
    if not normalized.lower().endswith(".csv"):
        normalized = f"{normalized}.csv"
    return normalized


def _resolve_gui_defs(tab_name: str, gui_filter: Any) -> list[Any]:
    if not gui_filter:
        return []
    if isinstance(gui_filter, (list, tuple, set)):
        names = list(gui_filter)
    else:
        names = [gui_filter]
    defs = []
    for name in names:
        filt = get_filter(tab_name, str(name))
        if filt:
            defs.append(filt)
    return defs


def _first_gui_name(gui_filter: Any) -> str:
    if isinstance(gui_filter, (list, tuple, set)):
        return str(list(gui_filter)[0])
    return str(gui_filter)


def _resolve_join(gui_defs: list[Any]) -> tuple[str | None, str | None, str]:
    if not gui_defs:
        return None, None, "LEFT"
    join_table = None
    join_on = None
    join_type = "LEFT"
    for filt in gui_defs:
        if filt.join_table and filt.join_on:
            if join_table and filt.join_table != join_table:
                raise ValueError(
                    "Multiple join tables in gui filters are not supported yet."
                )
            join_table = filt.join_table
            join_on = filt.join_on
            join_type = filt.join_type or "LEFT"
    return join_table, join_on, join_type


def _preferred_tables(table_counts: dict[str, int]) -> list[str]:
    def score(item: tuple[str, int]) -> tuple[int, int]:
        name, count = item
        upper = name.upper()
        bonus = 0
        if upper.endswith(".URLS") or upper == "URLS":
            bonus = 2
        elif upper.endswith(".LINKS") or upper == "LINKS":
            bonus = 1
        return (bonus, count)

    return [name for name, _ in sorted(table_counts.items(), key=score, reverse=True)]


def _connect_derby(db_root: Path, derby_jars: list[Path]):
    try:
        import jaydebeapi  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "jaydebeapi is required for Derby support. Install with 'pip install -e .[derby]'."
        ) from exc

    ensure_java_home()
    jdbc_url = f"jdbc:derby:{db_root};create=false"
    return jaydebeapi.connect(
        "org.apache.derby.iapi.jdbc.AutoloadedDriver",
        jdbc_url,
        jars=[str(p) for p in derby_jars],
    )


def _fetch_column_names(conn, table: str) -> list[str]:
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} FETCH FIRST 1 ROWS ONLY")
    return [col[0] for col in cursor.description]


def zipfile_is_zip(path: Path) -> bool:
    try:
        return zipfile.is_zipfile(path)
    except OSError:
        return False


def _headers_from_blob(blob: Any) -> dict[str, list[str]]:
    if not blob:
        return {}
    try:
        length = blob.length()
    except Exception:
        length = 0
    if not length:
        return {}
    try:
        raw = bytes(blob.getBytes(1, int(length)))
    except Exception:
        return {}
    if not raw:
        return {}
    try:
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        text = raw.decode("utf-8", errors="replace")
        payload = json.loads(text)
    except Exception:
        return {}
    headers: dict[str, list[str]] = {}
    for header in payload.get("mHeaders", []):
        name = str(header.get("mName", "")).lower()
        values = header.get("mValue") or []
        if not name:
            continue
        for value in values:
            if value is None:
                continue
            headers.setdefault(name, []).append(str(value))
    return headers


def _split_link_header(value: str) -> list[str]:
    segments: list[str] = []
    buf = ""
    in_quotes = False
    for ch in value:
        if ch == "\"":
            in_quotes = not in_quotes
        if ch == "," and not in_quotes:
            segments.append(buf)
            buf = ""
            continue
        buf += ch
    if buf:
        segments.append(buf)
    return segments


def _parse_link_headers(values: list[str]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for value in values:
        for segment in _split_link_header(value):
            part = segment.strip()
            if not part or "<" not in part:
                continue
            url_end = part.find(">")
            if url_end == -1:
                continue
            url = part[1:url_end].strip()
            params_str = part[url_end + 1 :]
            params: dict[str, str] = {}
            for token in params_str.split(";"):
                token = token.strip()
                if not token:
                    continue
                if "=" in token:
                    key, val = token.split("=", 1)
                    params[key.strip().lower()] = val.strip().strip("\"")
                else:
                    params[token.lower()] = ""
            links.append({"url": url, "params": params})
    return links


def _extract_link_rel(links: list[dict[str, Any]], rel: str) -> Optional[str]:
    target = rel.lower()
    for link in links:
        rels = link.get("params", {}).get("rel", "")
        tokens = re.split(r"\s+", rels.strip()) if rels else []
        if target in [t.lower() for t in tokens]:
            return link.get("url")
    return None


def _extract_hreflang(links: list[dict[str, Any]]) -> tuple[Optional[str], Optional[str]]:
    for link in links:
        params = link.get("params", {})
        rels = params.get("rel", "")
        rel_tokens = re.split(r"\s+", rels.strip()) if rels else []
        hreflang = params.get("hreflang")
        if hreflang and ("alternate" in [t.lower() for t in rel_tokens] or not rel_tokens):
            return hreflang, link.get("url")
    return None, None


def _extract_header_value(
    extract: dict[str, Any], headers: dict[str, list[str]], links: list[dict[str, Any]]
) -> Optional[str]:
    kind = extract.get("type")
    if kind == "link_rel":
        rel = str(extract.get("rel", "")).lower()
        return _extract_link_rel(links, rel) if rel else None
    if kind == "hreflang_lang":
        return _extract_hreflang(links)[0]
    if kind == "hreflang_url":
        return _extract_hreflang(links)[1]
    return None


_LINKS_BASE_SELECT = (
    "SELECT s.ENCODED_URL AS SOURCE, "
    "d.ENCODED_URL AS DESTINATION, "
    "l.LINK_TEXT, l.ALT_TEXT, l.HREF_LANG, l.NOFOLLOW, l.UGC, l.SPONSORED, "
    "l.TARGET, l.NOOPENER, l.NOREFERRER, "
    "l.PATH_TYPE, l.ELEMENT_PATH, l.ELEMENT_POSITION, "
    "l.LINK_TYPE, l.SCOPE, l.ORIGIN, "
    "u.RESPONSE_CODE AS DEST_STATUS_CODE, u.RESPONSE_MSG AS DEST_STATUS "
    "FROM APP.LINKS l "
    "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
    "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
    "LEFT JOIN APP.URLS u ON u.ENCODED_URL = d.ENCODED_URL"
)


def _link_row_to_dict(row: tuple[Any, ...]) -> dict[str, Any]:
    (
        source,
        destination,
        link_text,
        alt_text,
        hreflang,
        nofollow,
        ugc,
        sponsored,
        target,
        noopener,
        noreferrer,
        path_type,
        element_path,
        element_position,
        link_type,
        scope,
        origin,
        dest_status_code,
        dest_status,
    ) = row

    nofollow_bool = _normalize_bool(nofollow)
    ugc_bool = _normalize_bool(ugc)
    sponsored_bool = _normalize_bool(sponsored)
    noopener_bool = _normalize_bool(noopener)
    noreferrer_bool = _normalize_bool(noreferrer)
    follow = None if nofollow_bool is None else not nofollow_bool
    rel = _build_rel(nofollow_bool, ugc_bool, sponsored_bool, noopener_bool, noreferrer_bool)
    link_type_name = _link_type_name(link_type)
    if link_type_name is None and link_type is not None:
        link_type_name = str(link_type)

    return {
        "Type": link_type_name,
        "Source": _safe_text(source),
        "Destination": _safe_text(destination),
        "Alt Text": _safe_text(alt_text),
        "Anchor": _safe_text(link_text),
        "Status Code": dest_status_code,
        "Status": _safe_text(dest_status),
        "Follow": follow,
        "Target": _safe_text(target),
        "Rel": rel,
        "Path Type": path_type,
        "Link Path": _safe_text(element_path),
        "Link Position": element_position,
        "hreflang": _safe_text(hreflang),
        "Link Type": link_type,
        "Scope": scope,
        "Origin": origin,
        "NoFollow": nofollow_bool,
        "UGC": ugc_bool,
        "Sponsored": sponsored_bool,
        "Noopener": noopener_bool,
        "Noreferrer": noreferrer_bool,
    }


def _safe_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _normalize_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n", ""}:
        return False
    return bool(value)


def _build_rel(
    nofollow: Optional[bool],
    ugc: Optional[bool],
    sponsored: Optional[bool],
    noopener: Optional[bool],
    noreferrer: Optional[bool],
) -> str:
    tokens: list[str] = []
    if nofollow:
        tokens.append("nofollow")
    if ugc:
        tokens.append("ugc")
    if sponsored:
        tokens.append("sponsored")
    if noopener:
        tokens.append("noopener")
    if noreferrer:
        tokens.append("noreferrer")
    return " ".join(tokens)


def _link_type_name(value: Any) -> Optional[str]:
    try:
        code = int(value)
    except (TypeError, ValueError):
        return None
    return _LINK_TYPE_NAMES.get(code)


_LINK_TYPE_NAMES = {
    1: "Hyperlink",
    6: "Canonical",
    8: "Rel Prev",
    10: "Rel Next",
    12: "Hreflang (HTTP)",
    13: "Hreflang",
}
