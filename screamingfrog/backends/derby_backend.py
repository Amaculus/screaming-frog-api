from __future__ import annotations

import gzip
import json
import os
import re
import tempfile
import zipfile
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence
from urllib.parse import urljoin, urlparse, urlunparse

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
_COOKIE_TAB_KEYS = {
    "all_cookies.csv",
    "cookie_summary.csv",
}
_LANGUAGE_TAB_KEYS = {
    "spelling_and_grammar_errors.csv",
    "spelling_and_grammar_errors_report_summary.csv",
}
_STRUCTURED_DATA_TAB_KEYS = {
    "structured_data_all.csv",
    "structured_data_contains_structured_data.csv",
    "structured_data_jsonld_urls.csv",
    "structured_data_microdata_urls.csv",
    "structured_data_missing.csv",
    "structured_data_parse_errors.csv",
    "structured_data_rdfa_urls.csv",
    "structured_data_rich_result_feature_detected.csv",
    "structured_data_rich_result_validation_errors.csv",
    "structured_data_rich_result_validation_warnings.csv",
    "structured_data_validation_errors.csv",
    "structured_data_validation_warnings.csv",
    "structured_data_parse_error_report.csv",
    "contains_structured_data_detailed_report.csv",
}
_CHAIN_MAX_HOPS = 10
_FETCH_BATCH_SIZE = 1000


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
        self._internal_is_internal_col = _resolve_column_name(
            self._internal_columns, "IS_INTERNAL"
        )
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
        if self._internal_expr_selects:
            table_alias = "sf_internal"
            select_parts = [f"{table_alias}.*"]
            select_parts.extend(
                f"{expr} AS {alias}" for alias, _csv_col, expr in self._internal_expr_selects
            )
            sql = f"SELECT {', '.join(select_parts)} FROM {self._table} {table_alias}"
        else:
            sql = f"SELECT * FROM {self._table}"
        where_parts: list[str] = []
        params: list[Any] = []
        internal_clause = self._internal_only_clause()
        if internal_clause:
            where_parts.append(internal_clause)
        post_filters: dict[str, Any] = {}
        if filters:
            alias_map = getattr(self, "_internal_alias_map", None) or getattr(
                self, "_column_map", {}
            )
            where, filter_params, post_filters = _compile_internal_filters(
                filters,
                alias_map,
                getattr(self, "_internal_expr_selects", []),
                getattr(self, "_internal_header_extract_map", {}),
            )
            if where:
                where_parts.append(where)
            params.extend(filter_params)
        if where_parts:
            sql = f"{sql} WHERE {' AND '.join(where_parts)}"
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        columns = [desc[0] for desc in cursor.description or self._internal_columns]
        column_set = {str(col) for col in columns}
        active_expr_aliases = [
            (alias, csv_col)
            for alias, csv_col, _ in self._internal_expr_selects
            if alias in column_set
        ]
        active_aliases = [
            (csv_col, db_col)
            for csv_col, db_col in self._internal_alias_map.items()
            if db_col in column_set
        ]
        header_blob_columns: dict[str, str] = {}
        for extract in self._internal_header_extract_map.values():
            blob_col = _header_extract_column(extract)
            actual_col = _resolve_column_name(columns, blob_col)
            if actual_col:
                header_blob_columns[blob_col] = actual_col

        for row in _iter_cursor_rows(cursor):
            data = {col: val for col, val in zip(columns, row)}
            for alias, csv_col in active_expr_aliases:
                value = data.pop(alias, None)
                data.setdefault(csv_col, value)
            # Expose CSV-style aliases (e.g., "Status Code") for mapped direct columns.
            for csv_col, db_col in active_aliases:
                data.setdefault(csv_col, data.get(db_col))
            if self._internal_header_extract_map:
                parsed_headers: dict[str, dict[str, list[str]]] = {}
                parsed_links: dict[str, list[dict[str, Any]]] = {}
                for csv_col, extract in self._internal_header_extract_map.items():
                    if csv_col in data:
                        continue
                    blob_col = _header_extract_column(extract)
                    actual_col = header_blob_columns.get(blob_col)
                    headers: dict[str, list[str]] = {}
                    links: list[dict[str, Any]] = []
                    if actual_col:
                        if blob_col not in parsed_headers:
                            parsed_headers[blob_col] = _headers_from_blob(data.get(actual_col))
                            parsed_links[blob_col] = _parse_link_headers(
                                parsed_headers[blob_col].get("link", [])
                            ) if parsed_headers[blob_col] else []
                        headers = parsed_headers.get(blob_col, {})
                        links = parsed_links.get(blob_col, [])
                    data[csv_col] = _extract_header_value(extract, headers, links)
            if post_filters and not _row_matches_filters(data, post_filters):
                continue
            yield InternalPage.from_data(data, copy_data=False)

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
        for row in _iter_cursor_rows(cursor):
            yield Link.from_row(_link_row_to_dict(row))

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        if table != "internal":
            raise NotImplementedError("Derby backend only supports 'internal' in Phase 1")
        sql = f"SELECT COUNT(*) FROM {self._table}"
        where_parts: list[str] = []
        params: list[Any] = []
        internal_clause = self._internal_only_clause()
        if internal_clause:
            where_parts.append(internal_clause)
        if filters:
            alias_map = getattr(self, "_internal_alias_map", None) or getattr(
                self, "_column_map", {}
            )
            where, filter_params, post_filters = _compile_internal_filters(
                filters,
                alias_map,
                getattr(self, "_internal_expr_selects", []),
                getattr(self, "_internal_header_extract_map", {}),
            )
            if post_filters:
                return sum(1 for _ in self.get_internal(filters=filters))
            if where:
                where_parts.append(where)
            params.extend(filter_params)
        if where_parts:
            sql = f"{sql} WHERE {' AND '.join(where_parts)}"
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
        internal_clause = self._internal_only_clause()
        if internal_clause:
            sql = f"{sql} WHERE {internal_clause}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return cursor.fetchone()[0]

    def _internal_only_clause(self) -> str | None:
        column = getattr(self, "_internal_is_internal_col", None)
        if not column:
            column = _resolve_column_name(
                getattr(self, "_internal_columns", []), "IS_INTERNAL"
            )
        if not column:
            return None
        return f"{column} = TRUE"

    def list_tabs(self) -> list[str]:
        return sorted(_normalize_tab_name(name) for name in self._mapping.keys())

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.pop("__gui__", None)
        normalized = _normalize_tab_name(tab_name)
        if normalized in _CHAIN_TAB_KEYS:
            yield from self._get_chain_tab(normalized, filters)
            return
        if normalized in _COOKIE_TAB_KEYS:
            yield from self._get_cookie_tab(normalized, filters)
            return
        if normalized in _LANGUAGE_TAB_KEYS:
            yield from self._get_language_tab(normalized, filters)
            return
        if normalized in _STRUCTURED_DATA_TAB_KEYS:
            yield from self._get_structured_data_tab(normalized, filters)
            return
        table, entries, gui_defs, supplementary = _resolve_tab_entries(
            self._mapping, tab_name, gui_filter
        )
        if not entries:
            raise ValueError(f"No columns mapped for tab: {tab_name}")
        select_items: list[str] = []
        csv_columns: list[str] = []
        entry_indexes: list[int | None] = []
        header_indexes: dict[str, int] = {}
        blob_extract_indexes: dict[str, int] = {}
        derived_extract_indexes: dict[str, int] = {}
        encoded_url_index: int | None = None
        blob_checks = _resolve_blob_checks(gui_defs)
        blob_indexes: dict[str, int] = {}

        if supplementary and _table_supports_encoded_url(table):
            select_items.append("ENCODED_URL")
            encoded_url_index = len(select_items) - 1

        for entry in entries:
            if entry.get("header_extract"):
                blob_col = _header_extract_column(entry["header_extract"])
                if blob_col not in header_indexes:
                    select_items.append(blob_col)
                    header_indexes[blob_col] = len(select_items) - 1
                entry_indexes.append(None)
                csv_columns.append(entry["csv_column"])
                continue
            if entry.get("blob_extract"):
                blob_col = str(entry.get("db_column") or "")
                if blob_col and blob_col not in blob_extract_indexes:
                    select_items.append(blob_col)
                    blob_extract_indexes[blob_col] = len(select_items) - 1
                entry_indexes.append(None)
                csv_columns.append(entry["csv_column"])
                continue
            if entry.get("derived_extract"):
                for source_col in _derived_extract_columns(entry):
                    if source_col not in derived_extract_indexes:
                        select_items.append(source_col)
                        derived_extract_indexes[source_col] = len(select_items) - 1
                entry_indexes.append(None)
                csv_columns.append(entry["csv_column"])
                continue

            expr = entry.get("db_expression")
            if expr:
                if _is_null_expression(expr):
                    entry_indexes.append(None)
                    csv_columns.append(entry["csv_column"])
                    continue
                select_items.append(_normalize_select_expression(expr))
            else:
                select_items.append(entry["db_column"])
            entry_indexes.append(len(select_items) - 1)
            csv_columns.append(entry["csv_column"])

        for blob_column, _ in blob_checks:
            if blob_column in blob_indexes:
                continue
            try:
                idx = select_items.index(blob_column)
            except ValueError:
                select_items.append(blob_column)
                idx = len(select_items) - 1
            blob_indexes[blob_column] = idx

        if not select_items:
            select_items.append("1")
        select_cols = ", ".join(select_items)
        join_sql = ""
        where_parts: list[str] = []
        params: list[Any] = []

        join_table, join_on, join_type = _resolve_join(gui_defs)
        if join_table and join_on:
            join_sql = f" {join_type} JOIN {join_table} j ON {join_on}"

        sql = f"SELECT {select_cols} FROM {table}{join_sql}"
        params: list[Any] = []
        post_filters: dict[str, Any] = {}
        if filters:
            where, params, post_filters = _build_where_from_entries(
                filters, entries, supplementary
            )
            if where:
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
        for row in _iter_cursor_rows(cursor):
            if blob_checks and not _row_matches_blob_patterns(row, blob_checks, blob_indexes):
                continue
            parsed_headers: dict[str, dict[str, list[str]]] = {}
            parsed_links: dict[str, list[dict[str, Any]]] = {}
            output: dict[str, Any] = {}
            for entry, idx, column in zip(entries, entry_indexes, csv_columns):
                if entry.get("header_extract"):
                    blob_col = _header_extract_column(entry["header_extract"])
                    if blob_col not in parsed_headers:
                        blob_idx = header_indexes.get(blob_col)
                        if blob_idx is None:
                            parsed_headers[blob_col] = {}
                            parsed_links[blob_col] = []
                        else:
                            parsed_headers[blob_col] = _headers_from_blob(row[blob_idx])
                            parsed_links[blob_col] = _parse_link_headers(
                                parsed_headers[blob_col].get("link", [])
                            ) if parsed_headers[blob_col] else []
                    output[column] = _extract_header_value(
                        entry["header_extract"],
                        parsed_headers.get(blob_col, {}),
                        parsed_links.get(blob_col, []),
                    )
                elif entry.get("blob_extract"):
                    blob_col = str(entry.get("db_column") or "")
                    blob_idx = blob_extract_indexes.get(blob_col)
                    output[column] = _extract_blob_value(
                        entry["blob_extract"],
                        row[blob_idx] if blob_idx is not None else None,
                    )
                elif entry.get("derived_extract"):
                    source_values = {
                        source_col: row[idx]
                        for source_col, idx in derived_extract_indexes.items()
                        if idx < len(row)
                    }
                    output[column] = _extract_derived_value(
                        entry["derived_extract"],
                        source_values,
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
            if post_filters and not _row_matches_filters(output, post_filters):
                continue
            yield output

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT * FROM {table}")
        columns = [desc[0] for desc in cursor.description or []]
        for row in _iter_cursor_rows(cursor):
            yield {col: val for col, val in zip(columns, row)}

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        cursor = self._conn.cursor()
        cursor.execute(query, list(params or []))
        columns = [desc[0] for desc in cursor.description or []]
        for row in _iter_cursor_rows(cursor):
            yield {col: val for col, val in zip(columns, row)}

    def _get_cookie_tab(
        self, tab_key: str, filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        columns = _tab_columns(self._mapping, tab_key)
        norm_filters = _normalize_filters(filters)
        address_values = _filter_values(norm_filters, "address")
        cursor = self._conn.cursor()
        sql = (
            "SELECT ENCODED_URL, COOKIE_COLLECTION FROM APP.URLS "
            "WHERE COOKIE_COLLECTION IS NOT NULL"
        )
        params: list[Any] = []
        if address_values:
            placeholders = ", ".join(["?"] * len(address_values))
            sql += f" AND ENCODED_URL IN ({placeholders})"
            params.extend(address_values)
        cursor.execute(sql, params)

        if tab_key == "all_cookies.csv":
            for encoded_url, cookie_blob in _iter_cursor_rows(cursor):
                for row in _iter_cookie_rows(encoded_url, cookie_blob):
                    if _row_matches_filters(row, norm_filters):
                        yield {column: row.get(column) for column in columns}
            return

        summary_rows = _build_cookie_summary_rows(_iter_cursor_rows(cursor))
        for row in summary_rows:
            if _row_matches_filters(row, norm_filters):
                yield {column: row.get(column) for column in columns}

    def _get_language_tab(
        self, tab_key: str, filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        columns = _tab_columns(self._mapping, tab_key)
        norm_filters = _normalize_filters(filters)
        detailed_rows = list(self._iter_language_error_rows(norm_filters))

        if tab_key == "spelling_and_grammar_errors.csv":
            for row in detailed_rows:
                if _row_matches_filters(row, norm_filters):
                    yield {column: row.get(column) for column in columns}
            return

        summary_rows = _build_language_error_summary_rows(detailed_rows)
        for row in summary_rows:
            if _row_matches_filters(row, norm_filters):
                yield {column: row.get(column) for column in columns}

    def _iter_language_error_rows(
        self, norm_filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        address_values = _filter_values(norm_filters, "url", "address")
        cursor = self._conn.cursor()
        sql = (
            "SELECT ENCODED_URL, LANGUAGE_CODE, SPELLING_ERRORS, GRAMMAR_ERRORS, "
            "LANGUAGE_ERROR_DATA FROM APP.LANGUAGE_ERROR"
        )
        params: list[Any] = []
        if address_values:
            placeholders = ", ".join(["?"] * len(address_values))
            sql += f" WHERE ENCODED_URL IN ({placeholders})"
            params.extend(address_values)
        cursor.execute(sql, params)

        for encoded_url, language_code, spelling_errors, grammar_errors, error_blob in _iter_cursor_rows(cursor):
            payload = _decode_gzip_json_blob(error_blob)
            if not payload:
                continue
            errors = payload.get("errors") or []
            if not isinstance(errors, list) or not errors:
                continue

            grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
            counts: dict[tuple[Any, ...], int] = defaultdict(int)
            for raw_error in errors:
                if not isinstance(raw_error, dict):
                    continue
                signature = _language_error_signature(raw_error)
                grouped.setdefault(signature, raw_error)
                counts[signature] += 1

            lang = _safe_text(payload.get("langCode")) or _safe_text(language_code)
            spelling_total = payload.get("numSpellingErrors")
            grammar_total = payload.get("numGrammarErrors")
            spelling_total = spelling_errors if spelling_total is None else spelling_total
            grammar_total = grammar_errors if grammar_total is None else grammar_total

            for signature, raw_error in grouped.items():
                suggestions = raw_error.get("suggestions") or []
                if not isinstance(suggestions, list):
                    suggestions = [suggestions]
                row = {
                    "URL": encoded_url,
                    "Lang": lang,
                    "Spelling Errors": spelling_total,
                    "Grammar Errors": grammar_total,
                    "Error Type": _language_error_type(raw_error),
                    "Error Count": counts.get(signature, 0),
                    "Error": _safe_text(raw_error.get("ruleId"))
                    or _safe_text(raw_error.get("error")),
                    "Error with Context": None,
                    "Error Detail": _safe_text(raw_error.get("error")),
                    "Suggestions": ", ".join(
                        str(item) for item in suggestions if item not in {None, ""}
                    )
                    or None,
                    "Page Section": _language_page_section(raw_error.get("pageSection")),
                }
                yield row

    def _get_structured_data_tab(
        self, tab_key: str, filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        columns = _tab_columns(self._mapping, tab_key)
        norm_filters = _normalize_filters(filters)

        if tab_key == "contains_structured_data_detailed_report.csv":
            for row in self._iter_structured_data_detailed_rows(norm_filters):
                if _row_matches_filters(row, norm_filters):
                    yield {column: row.get(column) for column in columns}
            return

        for row in self._iter_structured_data_summary_rows(tab_key, norm_filters):
            if _row_matches_filters(row, norm_filters):
                yield {column: row.get(column) for column in columns}

    def _iter_structured_data_summary_rows(
        self, tab_key: str, norm_filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        address_values = _filter_values(norm_filters, "address", "url")
        cursor = self._conn.cursor()
        sql = (
            "SELECT u.ENCODED_URL, u.SERIALISED_STRUCTURED_DATA, u.PARSE_ERROR_MSG, "
            "i.RICH_RESULTS_TYPES, i.RICH_RESULTS_TYPE_ERRORS, i.RICH_RESULTS_TYPE_WARNINGS "
            "FROM APP.URLS u "
            "LEFT JOIN APP.URL_INSPECTION i ON i.ENCODED_URL = u.ENCODED_URL"
        )
        params: list[Any] = []
        if address_values:
            placeholders = ", ".join(["?"] * len(address_values))
            sql += f" WHERE u.ENCODED_URL IN ({placeholders})"
            params.extend(address_values)
        cursor.execute(sql, params)

        for encoded_url, data_blob, parse_error, rich_types, rich_errors, rich_warnings in _iter_cursor_rows(cursor):
            blocks = _parse_structured_data_blocks(data_blob)
            formats = [block["format"] for block in blocks if block.get("format")]
            format_set = set(formats)
            type_occurrences: list[str] = []
            for block in blocks:
                type_occurrences.extend(_extract_structured_data_types(block.get("text")))
            unique_types = _ordered_unique(type_occurrences)
            features = _parse_rich_result_features(rich_types)
            if not features:
                features = _derive_rich_result_features(type_occurrences)
            rich_error_count = _safe_int(rich_errors) or 0
            rich_warning_count = _safe_int(rich_warnings) or 0
            parse_error_text = _safe_text(parse_error)
            indexability, indexability_status = self._fetch_indexability_values(encoded_url)

            row = {
                "Address": encoded_url,
                "Errors": rich_error_count,
                "Warnings": rich_warning_count,
                "Rich Result Errors": rich_error_count,
                "Rich Result Warnings": rich_warning_count,
                "Rich Result Features": len(features),
                "Total Types": len(type_occurrences),
                "Unique Types": len(unique_types),
                "Indexability": indexability,
                "Indexability Status": indexability_status,
                "Parse Error": parse_error_text,
            }
            for index in range(6):
                row[f"Feature-{index + 1}"] = features[index] if index < len(features) else None
            for index in range(17):
                row[f"Type-{index + 1}"] = (
                    type_occurrences[index] if index < len(type_occurrences) else None
                )

            has_data = bool(blocks)
            include = False
            if tab_key in {"structured_data_all.csv", "structured_data_parse_error_report.csv"}:
                include = True
            elif tab_key == "structured_data_contains_structured_data.csv":
                include = has_data
            elif tab_key == "structured_data_jsonld_urls.csv":
                include = "JSONLD" in format_set
            elif tab_key == "structured_data_microdata_urls.csv":
                include = "MICRODATA" in format_set
            elif tab_key == "structured_data_rdfa_urls.csv":
                include = "RDFA" in format_set
            elif tab_key == "structured_data_missing.csv":
                include = not has_data
            elif tab_key == "structured_data_parse_errors.csv":
                include = bool(parse_error_text)
            elif tab_key == "structured_data_rich_result_feature_detected.csv":
                include = bool(features)
            elif tab_key in {
                "structured_data_rich_result_validation_errors.csv",
                "structured_data_validation_errors.csv",
            }:
                include = rich_error_count > 0
            elif tab_key in {
                "structured_data_rich_result_validation_warnings.csv",
                "structured_data_validation_warnings.csv",
            }:
                include = rich_warning_count > 0

            if not include:
                continue

            if tab_key == "structured_data_parse_error_report.csv":
                yield {
                    "Address": encoded_url,
                    "Parse Error": parse_error_text,
                }
                continue

            yield row

    def _iter_structured_data_detailed_rows(
        self, norm_filters: dict[str, Any]
    ) -> Iterator[dict[str, Any]]:
        address_values = _filter_values(norm_filters, "address", "url")
        cursor = self._conn.cursor()
        sql = (
            "SELECT ENCODED_URL, SERIALISED_STRUCTURED_DATA, PARSE_ERROR_MSG "
            "FROM APP.URLS WHERE SERIALISED_STRUCTURED_DATA IS NOT NULL"
        )
        params: list[Any] = []
        if address_values:
            placeholders = ", ".join(["?"] * len(address_values))
            sql += f" AND ENCODED_URL IN ({placeholders})"
            params.extend(address_values)
        cursor.execute(sql, params)

        for encoded_url, data_blob, _parse_error in _iter_cursor_rows(cursor):
            blocks = _parse_structured_data_blocks(data_blob)
            if not blocks:
                continue
            summary_features = []
            warning_count = 0
            error_count = 0
            term_map: dict[str, str] = {}
            term_index = 0
            for block in blocks:
                format_label = _structured_data_format_label(block.get("format"))
                text = block.get("text")
                if not text:
                    continue
                for subject, _predicate, object_value in _iter_structured_data_triples(text):
                    if subject not in term_map:
                        term_map[subject] = f"subject{term_index}"
                        term_index += 1
                    if object_value.startswith("_:") or object_value.startswith("<"):
                        if object_value not in term_map:
                            term_map[object_value] = f"subject{term_index}"
                            term_index += 1
                    normalized_object = term_map.get(
                        object_value, _normalize_structured_object(object_value)
                    )
                    row = {
                        "URL": encoded_url,
                        "Subject": term_map[subject],
                        "Predicate": format_label,
                        "Object": normalized_object,
                        "Errors": error_count,
                        "Warnings": warning_count,
                    }
                    for index in range(10):
                        row[f"Validation Type {index + 1}"] = (
                            summary_features[index] if index < len(summary_features) else None
                        )
                        row[f"Severity {index + 1}"] = None
                        row[f"Issue {index + 1}"] = None
                    yield row

    def _fetch_indexability_values(self, encoded_url: str) -> tuple[Any, Any]:
        idx_expr = None
        idx_status_expr = None
        for entry in self._mapping.get(_INTERNAL_MAPPING_KEY, []):
            if entry.get("csv_column") == "Indexability":
                idx_expr = entry.get("db_expression")
            if entry.get("csv_column") == "Indexability Status":
                idx_status_expr = entry.get("db_expression")
        if not idx_expr or not idx_status_expr:
            return None, None
        cursor = self._conn.cursor()
        cursor.execute(
            f"SELECT {idx_expr}, {idx_status_expr} FROM APP.URLS "
            "WHERE ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY",
            [encoded_url],
        )
        row = cursor.fetchone()
        if not row:
            return None, None
        return row[0], row[1]

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
            url = urljoin(base, text)
            return _strip_default_port(url)

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
        expr_str = _normalize_select_expression(expr)
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
        if not entries and "-" in filename:
            entries = mapping.get(filename.replace("-", ""))
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
        if (
            entry.get("db_expression")
            or entry.get("header_extract")
            or entry.get("blob_extract")
            or entry.get("derived_extract")
        ):
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


def _append_filter_clause(
    clauses: list[str],
    params: list[Any],
    sql_expr: str,
    expected: Any,
    *,
    wrap_expr: bool = False,
) -> None:
    expr = f"({sql_expr})" if wrap_expr else sql_expr
    if isinstance(expected, (list, tuple, set)):
        values = list(expected)
        if not values:
            clauses.append("1=0")
            return
        placeholders = ", ".join(["?"] * len(values))
        clauses.append(f"{expr} IN ({placeholders})")
        params.extend(values)
    elif expected is None:
        clauses.append(f"{expr} IS NULL")
    else:
        clauses.append(f"{expr} = ?")
        params.append(expected)


def _compile_internal_filters(
    filters: dict[str, Any],
    alias_map: dict[str, str],
    expr_selects: Sequence[tuple[str, str, str]],
    header_extract_map: dict[str, dict[str, Any]],
) -> tuple[str, list[Any], dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    post_filters: dict[str, Any] = {}
    direct_map = {_normalize_key(csv_col): db_col for csv_col, db_col in alias_map.items()}
    expr_map = {
        _normalize_key(csv_col): expr for _alias, csv_col, expr in expr_selects
    }
    post_map = {_normalize_key(csv_col) for csv_col in header_extract_map}

    for key, expected in filters.items():
        lookup = _normalize_key(str(key))
        expr = expr_map.get(lookup)
        if expr:
            _append_filter_clause(clauses, params, expr, expected, wrap_expr=True)
            continue
        column = direct_map.get(lookup)
        if column:
            _append_filter_clause(clauses, params, column, expected)
            continue
        if lookup in post_map:
            post_filters[key] = expected
            continue
        _append_filter_clause(clauses, params, str(key), expected)

    return " AND ".join(clauses), params, post_filters


def _build_where_from_entries(
    filters: dict[str, Any],
    entries: list[dict[str, Any]],
    supplementary: Optional[list[dict[str, Any]]] = None,
) -> tuple[str, list[Any], dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    post_filters: dict[str, Any] = {}
    field_map: dict[str, tuple[str, str | None]] = {}

    for entry in entries:
        csv_key = _normalize_key(entry.get("csv_column", ""))
        if not csv_key or csv_key in field_map:
            continue
        if entry.get("header_extract") or entry.get("blob_extract") or entry.get("derived_extract"):
            field_map[csv_key] = ("post", None)
            continue
        expr = entry.get("db_expression")
        if expr:
            if _is_null_expression(expr):
                field_map[csv_key] = ("post", None)
                continue
            field_map[csv_key] = ("expr", _normalize_select_expression(expr))
            continue
        column = entry.get("db_column")
        if column:
            field_map[csv_key] = ("column", str(column))

    for entry in supplementary or []:
        csv_key = _normalize_key(entry.get("csv_column", ""))
        if csv_key and csv_key not in field_map:
            field_map[csv_key] = ("post", None)

    for key, expected in filters.items():
        lookup = _normalize_key(str(key))
        mode, sql_expr = field_map.get(lookup, ("raw", str(key)))
        if mode == "post":
            post_filters[key] = expected
            continue
        _append_filter_clause(
            clauses,
            params,
            str(sql_expr or key),
            expected,
            wrap_expr=mode == "expr",
        )

    return " AND ".join(clauses), params, post_filters


def _normalize_select_expression(expr: Any) -> str:
    text = str(expr).strip()
    if text.upper() == "NULL":
        return "CAST(NULL AS VARCHAR(1))"

    def _replace_numeric_cast(match: re.Match[str]) -> str:
        token = match.group(1)
        if token.upper() == "NULL":
            return match.group(0)
        return f"TRIM(CHAR({token}))"

    text = re.sub(
        r"(?i)CAST\(\s*([A-Z_][A-Z0-9_\.]*)\s+AS\s+VARCHAR\s*\(\s*\d+\s*\)\s*\)",
        _replace_numeric_cast,
        text,
    )
    upper = text.upper()
    if "ELSE NULL END" in upper and (
        "VARCHAR" in upper or "CHAR(" in upper or "||" in text
    ):
        text = re.sub(
            r"(?i)ELSE\s+NULL\s+END",
            "ELSE CAST(NULL AS VARCHAR(1)) END",
            text,
        )

    # Derby rejects correlated outer-table references inside subquery JOIN ON clauses.
    # The mapping uses patterns like:
    #   FROM APP.URLS u JOIN APP.UNIQUE_URLS d ON d.ID = APP.LINKS.DST_ID
    #   WHERE u.ENCODED_URL = d.ENCODED_URL
    # which Derby raises: "Column 'APP.LINKS.DST_ID' is either not in any table in
    # the FROM list of the subquery".
    # Rewrite to a WHERE-only equivalent that moves the correlated reference out of
    # the JOIN ON clause — correlated references in WHERE are supported by Derby:
    #   FROM APP.URLS u
    #   WHERE u.ENCODED_URL = (SELECT uu.ENCODED_URL FROM APP.UNIQUE_URLS uu
    #                          WHERE uu.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY)
    for col_ref, col_name in (("DST_ID", "DST_ID"), ("SRC_ID", "SRC_ID")):
        text = re.sub(
            rf"(?i)FROM\s+APP\.URLS\s+(\w+)"
            rf"\s+JOIN\s+APP\.UNIQUE_URLS\s+\w+"
            rf"\s+ON\s+\w+\.ID\s*=\s*APP\.LINKS\.{col_ref}"
            rf"\s+WHERE\s+\1\.ENCODED_URL\s*=\s*\w+\.ENCODED_URL",
            lambda m, cn=col_name: (
                f"FROM APP.URLS {m.group(1)} "
                f"WHERE {m.group(1)}.ENCODED_URL = ("
                f"SELECT uu.ENCODED_URL FROM APP.UNIQUE_URLS uu "
                f"WHERE uu.ID = APP.LINKS.{cn} FETCH FIRST 1 ROWS ONLY)"
            ),
            text,
        )

    return text


def _is_null_expression(expr: Any) -> bool:
    return str(expr).strip().upper() == "NULL"


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


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


def _resolve_blob_checks(gui_defs: list[Any]) -> list[tuple[str, bytes]]:
    checks: list[tuple[str, bytes]] = []
    for filt in gui_defs:
        column = getattr(filt, "blob_column", None)
        pattern = getattr(filt, "blob_pattern", None)
        if not column or not pattern:
            continue
        if isinstance(pattern, bytes):
            checks.append((str(column), pattern))
        else:
            checks.append((str(column), str(pattern).encode("utf-8")))
    return checks


def _tab_columns(mapping: dict[str, Any], tab_key: str) -> list[str]:
    return [
        entry.get("csv_column")
        for entry in mapping.get(tab_key, [])
        if entry.get("csv_column")
    ]


def _normalize_filters(filters: dict[str, Any]) -> dict[str, Any]:
    return {_normalize_key(str(key)): value for key, value in filters.items()}


def _filter_values(norm_filters: dict[str, Any], *keys: str) -> list[Any]:
    values: list[Any] = []
    for key in keys:
        expected = norm_filters.get(_normalize_key(key))
        if expected is None:
            continue
        if isinstance(expected, (list, tuple, set)):
            values.extend(list(expected))
        else:
            values.append(expected)
    return values


def _row_matches_filters(row: dict[str, Any], norm_filters: dict[str, Any]) -> bool:
    if not norm_filters:
        return True
    row_lookup = {_normalize_key(str(key)): value for key, value in row.items()}
    for key, expected in norm_filters.items():
        actual = row_lookup.get(key)
        if isinstance(expected, (list, tuple, set)):
            if not any(_filter_value_matches(actual, item) for item in expected):
                return False
            continue
        if not _filter_value_matches(actual, expected):
            return False
    return True


def _filter_value_matches(actual: Any, expected: Any) -> bool:
    if expected is None:
        if actual is None:
            return True
        if isinstance(actual, str) and not actual.strip():
            return True
        return False
    if actual is None:
        return False
    if isinstance(actual, bool) or isinstance(expected, bool):
        actual_bool = _normalize_bool(actual)
        expected_bool = _normalize_bool(expected)
        return actual_bool is not None and actual_bool == expected_bool
    actual_int = _safe_int(actual)
    expected_int = _safe_int(expected)
    if actual_int is not None and expected_int is not None:
        return actual_int == expected_int
    return str(actual).strip().lower() == str(expected).strip().lower()


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


def _resolve_column_name(columns: Sequence[str], target: str) -> str | None:
    target_norm = str(target).strip().lower()
    for column in columns:
        if str(column).strip().lower() == target_norm:
            return str(column)
    return None


def _iter_cursor_rows(cursor, batch_size: int = _FETCH_BATCH_SIZE) -> Iterator[tuple[Any, ...]]:
    """Yield cursor rows in chunks to avoid loading full result sets into memory."""
    fetchmany = getattr(cursor, "fetchmany", None)
    if not callable(fetchmany):
        for row in cursor.fetchall():
            yield row
        return
    while True:
        rows = fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row


def zipfile_is_zip(path: Path) -> bool:
    try:
        return zipfile.is_zipfile(path)
    except OSError:
        return False


def _blob_bytes(blob: Any) -> bytes:
    if blob is None:
        return b""
    if isinstance(blob, (bytes, bytearray, memoryview)):
        return bytes(blob)
    try:
        length = int(blob.length())
    except Exception:
        return b""
    if length <= 0:
        return b""
    try:
        return bytes(blob.getBytes(1, length))
    except Exception:
        return b""


def _decode_gzip_json_blob(blob: Any) -> dict[str, Any]:
    raw = _blob_bytes(blob)
    if not raw:
        return {}
    try:
        return json.loads(gzip.decompress(raw).decode("utf-8", errors="replace"))
    except Exception:
        return {}


def _headers_from_blob(blob: Any) -> dict[str, list[str]]:
    if not blob:
        return {}
    raw = _blob_bytes(blob)
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


def _row_matches_blob_patterns(
    row: tuple[Any, ...],
    checks: list[tuple[str, bytes]],
    indexes: dict[str, int],
) -> bool:
    for column, pattern in checks:
        idx = indexes.get(column)
        if idx is None:
            return False
        if not _blob_contains(row[idx], pattern):
            return False
    return True


def _blob_contains(blob: Any, pattern: bytes) -> bool:
    if blob is None:
        return False
    if not pattern:
        return False
    raw = _blob_bytes(blob)
    if not raw:
        return False
    sample = raw[:512]
    return pattern.upper() in sample.upper()


_DEFAULT_PORTS = {"http": "80", "https": "443"}


def _strip_default_port(url: str) -> str:
    """Remove default port from URL (e.g. :443 for https, :80 for http)."""
    parsed = urlparse(url)
    default_port = _DEFAULT_PORTS.get(parsed.scheme.lower())
    if not default_port:
        return url

    netloc = parsed.netloc
    if not netloc:
        return url

    userinfo = ""
    hostport = netloc
    if "@" in hostport:
        userinfo, hostport = hostport.rsplit("@", 1)
        userinfo += "@"

    if hostport.startswith("["):
        end = hostport.find("]")
        if end < 0:
            return url
        host = hostport[: end + 1]
        remainder = hostport[end + 1 :]
        if not remainder.startswith(":"):
            return url
        port_text = remainder[1:]
        if not port_text.isdigit() or int(port_text) != int(default_port):
            return url
        new_netloc = f"{userinfo}{host}"
    else:
        if ":" not in hostport:
            return url
        host, sep, port_text = hostport.rpartition(":")
        if not sep or not host:
            return url
        if not port_text.isdigit() or int(port_text) != int(default_port):
            return url
        new_netloc = f"{userinfo}{host}"

    if new_netloc == netloc:
        return url
    new = parsed._replace(netloc=new_netloc)
    return urlunparse(new)


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
    if kind == "header_name":
        name = str(extract.get("name", "")).strip().lower()
        if not name:
            return None
        values = headers.get(name) or []
        if not values:
            return None
        return ", ".join(str(value) for value in values if value is not None) or None
    if kind == "link_rel":
        rel = str(extract.get("rel", "")).lower()
        return _extract_link_rel(links, rel) if rel else None
    if kind == "hreflang_lang":
        return _extract_hreflang(links)[0]
    if kind == "hreflang_url":
        return _extract_hreflang(links)[1]
    return None


def _header_extract_column(extract: dict[str, Any]) -> str:
    return str(extract.get("column") or "HTTP_RESPONSE_HEADER_COLLECTION")


def _extract_blob_value(extract: dict[str, Any], blob: Any) -> Any:
    kind = str(extract.get("type") or "").strip().lower()
    if kind == "cookie_count":
        payload = _decode_gzip_json_blob(blob)
        cookies = payload.get("mCookies") or []
        return len(cookies) if isinstance(cookies, list) else None
    return None


def _derived_extract_columns(entry: dict[str, Any]) -> list[str]:
    extract = entry.get("derived_extract") or {}
    columns = list(extract.get("columns") or [])
    primary = entry.get("db_column")
    if primary:
        columns.insert(0, str(primary))
    deduped: list[str] = []
    seen: set[str] = set()
    for column in columns:
        text = str(column or "").strip()
        if text and text not in seen:
            deduped.append(text)
            seen.add(text)
    return deduped


def _extract_derived_value(extract: dict[str, Any], values: dict[str, Any]) -> Any:
    kind = str(extract.get("type") or "").strip().lower()
    if kind == "redirect_url":
        address = _safe_text(values.get("ENCODED_URL"))
        num_meta = _safe_int(values.get("NUM_METAREFRESH"))
        meta_url = _safe_text(values.get("META_FULL_URL_1")) or _safe_text(
            values.get("META_FULL_URL_2")
        )
        if num_meta and meta_url:
            return urljoin(address or "", meta_url)

        code = _safe_int(values.get("RESPONSE_CODE"))
        headers_blob = values.get("HTTP_RESPONSE_HEADER_COLLECTION")
        if code is not None and 300 <= code < 400 and headers_blob is not None:
            headers = _headers_from_blob(headers_blob)
            locations = headers.get("location", [])
            if locations:
                return urljoin(address or "", locations[0])
        return None
    return None


def _iter_cookie_rows(encoded_url: Any, cookie_blob: Any) -> Iterator[dict[str, Any]]:
    payload = _decode_gzip_json_blob(cookie_blob)
    cookies = payload.get("mCookies") or []
    if not isinstance(cookies, list):
        return
    address = _safe_text(encoded_url)
    for cookie in cookies:
        if not isinstance(cookie, dict):
            continue
        yield {
            "Address": address,
            "Cookie Name": _safe_text(cookie.get("mName")),
            "Cookie Value": _safe_text(cookie.get("mValue")),
            "Domain": _safe_text(cookie.get("mDomain")),
            "Path": _safe_text(cookie.get("mPath")),
            "Expiration Time": _cookie_expiration_text(cookie.get("mExpirationTime")),
            "Secure": _normalize_bool(cookie.get("mIsSecure")),
            "HttpOnly": _normalize_bool(cookie.get("mIsHttpOnly")),
        }


def _build_cookie_summary_rows(
    source_rows: Iterator[tuple[Any, ...]],
) -> list[dict[str, Any]]:
    aggregates: dict[tuple[Any, ...], dict[str, Any]] = {}
    for encoded_url, cookie_blob in source_rows:
        for row in _iter_cookie_rows(encoded_url, cookie_blob):
            key = (
                row.get("Cookie Name"),
                row.get("Domain"),
                row.get("Path"),
                row.get("Expiration Time"),
                row.get("Secure"),
                row.get("HttpOnly"),
            )
            current = aggregates.get(key)
            if current is None:
                current = {
                    "Cookie Name": row.get("Cookie Name"),
                    "Domain": row.get("Domain"),
                    "Path": row.get("Path"),
                    "Expiration Time": row.get("Expiration Time"),
                    "Secure": row.get("Secure"),
                    "HttpOnly": row.get("HttpOnly"),
                    "Occurrences": 0,
                    "Sample URL": row.get("Address"),
                }
                aggregates[key] = current
            current["Occurrences"] = int(current.get("Occurrences") or 0) + 1
    return list(aggregates.values())


def _cookie_expiration_text(value: Any) -> Optional[str]:
    seconds = _safe_int(value)
    if seconds is None:
        return None
    if seconds < 0:
        return "Session"
    if seconds == 0:
        return "0 Seconds"
    units = (
        ("Day", 24 * 60 * 60),
        ("Hour", 60 * 60),
        ("Minute", 60),
        ("Second", 1),
    )
    for label, size in units:
        count = seconds // size
        if count >= 1:
            suffix = "" if count == 1 else "s"
            return f"{count} {label}{suffix}"
    return "0 Seconds"


def _language_error_signature(error: dict[str, Any]) -> tuple[Any, ...]:
    suggestions = error.get("suggestions") or []
    if not isinstance(suggestions, list):
        suggestions = [suggestions]
    return (
        _safe_text(error.get("ruleId")),
        _safe_text(error.get("errorType")),
        _safe_text(error.get("error")),
        tuple(str(item) for item in suggestions if item not in {None, ""}),
        _safe_text(error.get("pageSection")),
    )


def _language_error_type(error: dict[str, Any]) -> Optional[str]:
    raw = str(error.get("errorType") or "").strip().upper()
    if raw in {"TYPO", "SPELLING", "MISSPELLING"}:
        return "Spelling"
    if not raw:
        return None
    return "Grammar"


def _language_page_section(value: Any) -> Optional[str]:
    raw = str(value or "").strip().upper()
    mapping = {
        "CONTENT": "Page Body",
        "TITLE": "Title",
        "META_DESCRIPTION": "Meta Description",
        "META_KEYWORDS": "Meta Keywords",
        "HEADINGS": "Headings",
    }
    if raw in mapping:
        return mapping[raw]
    if not raw:
        return None
    return raw.replace("_", " ").title()


def _build_language_error_summary_rows(
    rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    total_urls = len({row.get("URL") for row in rows if row.get("URL")})
    aggregates: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("Error"),
            row.get("Error Type"),
            row.get("Error Detail"),
        )
        current = aggregates.get(key)
        if current is None:
            current = {
                "Error": row.get("Error"),
                "Error Type": row.get("Error Type"),
                "Error Count": 0,
                "URLs Affected": 0,
                "Coverage %": 0.0,
                "Error Detail": row.get("Error Detail"),
                "Sample URL": row.get("URL"),
            }
            current["_urls"] = set()
            aggregates[key] = current
        current["Error Count"] = int(current.get("Error Count") or 0) + int(
            row.get("Error Count") or 0
        )
        if row.get("URL"):
            current["_urls"].add(row["URL"])
    for current in aggregates.values():
        urls_affected = len(current.pop("_urls", set()))
        current["URLs Affected"] = urls_affected
        current["Coverage %"] = round(
            (urls_affected / total_urls) * 100, 2
        ) if total_urls else 0.0
    return list(aggregates.values())


def _parse_structured_data_blocks(blob: Any) -> list[dict[str, Any]]:
    raw = _blob_bytes(blob)
    if not raw:
        return []
    blocks: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for index in range(len(raw) - 1):
        if raw[index : index + 2] != b"\x1f\x8b":
            continue
        try:
            inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
            text_bytes = inflater.decompress(raw[index:])
        except Exception:
            continue
        if not text_bytes:
            continue
        text = text_bytes.decode("utf-8", errors="replace")
        format_key = _structured_data_format_from_context(raw, index)
        signature = (format_key, text[:256])
        if signature in seen:
            continue
        seen.add(signature)
        blocks.append({"format": format_key, "text": text})
    return blocks


def _structured_data_format_from_context(raw: bytes, index: int) -> Optional[str]:
    start = max(0, index - 96)
    context = raw[start:index].upper()
    for token in (b"JSONLD", b"MICRODATA", b"RDFA"):
        if token in context:
            return token.decode("ascii")
    return None


def _structured_data_format_label(value: Any) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if raw == "JSONLD":
        return "JSON-LD"
    if raw == "MICRODATA":
        return "Microdata"
    if raw == "RDFA":
        return "RDFa"
    return _safe_text(value)


def _extract_structured_data_types(text: Any) -> list[str]:
    if not text:
        return []
    matches = re.findall(
        r"<http://www\.w3\.org/1999/02/22-rdf-syntax-ns#type>\s+<https?://schema\.org/([^>]+)>",
        str(text),
    )
    return [match.rsplit("/", 1)[-1] for match in matches if match]


def _ordered_unique(values: Sequence[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        output.append(value)
        seen.add(value)
    return output


def _parse_rich_result_features(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        text = str(value).strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                decoded = json.loads(text)
                if isinstance(decoded, list):
                    raw_values = decoded
                else:
                    raw_values = [decoded]
            except Exception:
                raw_values = re.split(r"[,\n;|]+", text)
        else:
            raw_values = re.split(r"[,\n;|]+", text)
    features: list[str] = []
    for raw in raw_values:
        token = _safe_text(raw)
        if not token:
            continue
        if not token.lower().startswith("google "):
            token = f"Google {token}"
        if token not in features:
            features.append(token)
    return features


def _derive_rich_result_features(types: Sequence[str]) -> list[str]:
    mapping = {
        "FAQPage": "Google FAQ",
        "BreadcrumbList": "Google Breadcrumb",
        "Review": "Google Review Snippet",
        "AggregateRating": "Google Review Snippet",
        "SoftwareApplication": "Google Software App",
        "SportsEvent": "Google Event",
        "Event": "Google Event",
        "Person": "Google Profile Page",
        "ProfilePage": "Google Profile Page",
        "Recipe": "Google Recipe",
        "VideoObject": "Google Video",
        "JobPosting": "Google Job Posting",
        "HowTo": "Google How-To",
        "QAPage": "Google Q&A",
        "Product": "Google Product",
        "NewsArticle": "Google Article",
        "Article": "Google Article",
    }
    features: list[str] = []
    for item in types:
        feature = mapping.get(str(item))
        if feature and feature not in features:
            features.append(feature)
    return features


def _iter_structured_data_triples(text: str) -> Iterator[tuple[str, str, str]]:
    for line in str(text).splitlines():
        cleaned = line.strip()
        if not cleaned or not cleaned.endswith("."):
            continue
        match = re.match(r"^(\S+)\s+<([^>]+)>\s+(.+?)\s+\.$", cleaned)
        if not match:
            continue
        yield match.group(1), match.group(2), match.group(3)


def _normalize_structured_object(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("<") and cleaned.endswith(">"):
        cleaned = cleaned[1:-1]
    if cleaned.startswith("\"") and cleaned.endswith("\""):
        cleaned = cleaned[1:-1]
    return cleaned


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
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
