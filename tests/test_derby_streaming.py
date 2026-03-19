from __future__ import annotations

import gzip
import json
from typing import Any

from screamingfrog.backends.derby_backend import DerbyBackend, _iter_cursor_rows


class _FakeCursor:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.description = [(col,) for col in columns]
        self._rows = list(rows)
        self._index = 0
        self._fetchone_index = 0
        self.fetchall_called = 0
        self.fetchone_called = 0
        self.fetchmany_calls: list[int] = []
        self.executed_sql: str | None = None
        self.executed_params: list[Any] | None = None

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        self.executed_sql = sql
        self.executed_params = list(params or [])

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        self.fetchmany_calls.append(size)
        if self._index >= len(self._rows):
            return []
        end = min(self._index + size, len(self._rows))
        chunk = self._rows[self._index : end]
        self._index = end
        return chunk

    def fetchall(self) -> list[tuple[Any, ...]]:
        self.fetchall_called += 1
        return []

    def fetchone(self) -> tuple[Any, ...] | None:
        self.fetchone_called += 1
        if self._fetchone_index >= len(self._rows):
            return None
        row = self._rows[self._fetchone_index]
        self._fetchone_index += 1
        return row


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _FakeCursor:
        return self._cursor


class _MultiCursorConnection:
    def __init__(self, cursors: list[_FakeCursor]) -> None:
        self._cursors = list(cursors)
        self._index = 0

    def cursor(self) -> _FakeCursor:
        if self._index >= len(self._cursors):
            raise AssertionError("No fake cursor left for connection.cursor()")
        cursor = self._cursors[self._index]
        self._index += 1
        return cursor


class _FakeBlob:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def length(self) -> int:
        return len(self._data)

    def getBytes(self, start: int, length: int) -> bytes:
        offset = max(0, start - 1)
        return self._data[offset : offset + length]


def _headers_blob(**headers: list[str]) -> _FakeBlob:
    payload = {
        "mHeaders": [
            {"mName": name, "mValue": list(values)}
            for name, values in headers.items()
        ]
    }
    return _FakeBlob(gzip.compress(json.dumps(payload).encode("utf-8")))


def _json_blob(payload: dict[str, Any]) -> _FakeBlob:
    return _FakeBlob(gzip.compress(json.dumps(payload).encode("utf-8")))


def test_iter_cursor_rows_uses_fetchmany_chunks() -> None:
    cursor = _FakeCursor(
        ["A"],
        [(1,), (2,), (3,), (4,), (5,)],
    )

    rows = list(_iter_cursor_rows(cursor, batch_size=2))

    assert rows == [(1,), (2,), (3,), (4,), (5,)]
    assert cursor.fetchall_called == 0
    assert cursor.fetchmany_calls == [2, 2, 2, 2]


def test_get_internal_streams_rows_without_fetchall() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL", "SF_EXPR_0", "SF_EXPR_1"],
        [
            ("https://example.com/", 200, True, "Indexable", "Indexable"),
            ("https://example.com/missing", 404, True, "Non-Indexable", "Noindex"),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._table = "APP.URLS"
    backend._conn = _FakeConnection(cursor)
    backend._column_map = {}
    backend._internal_columns = ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL"]
    backend._internal_is_internal_col = "IS_INTERNAL"
    backend._internal_expr_selects = [
        ("SF_EXPR_0", "Indexability", "CASE WHEN 1=1 THEN 'Indexable' END"),
        ("SF_EXPR_1", "Indexability Status", "CASE WHEN 1=1 THEN 'Indexable' END"),
    ]
    backend._internal_alias_map = {
        "Address": "ENCODED_URL",
        "Status Code": "RESPONSE_CODE",
    }
    backend._internal_header_extract_map = {}

    pages = list(backend.get_internal())

    assert len(pages) == 2
    assert pages[0].address == "https://example.com/"
    assert pages[0].status_code == 200
    assert pages[1].address == "https://example.com/missing"
    assert pages[1].status_code == 404
    assert pages[0].data["Indexability"] == "Indexable"
    assert pages[0].data["Indexability Status"] == "Indexable"
    assert pages[1].data["Indexability"] == "Non-Indexable"
    assert pages[1].data["Indexability Status"] == "Noindex"
    assert pages[0].data["Status Code"] == 200
    assert pages[1].data["Status Code"] == 404
    assert cursor.executed_sql == (
        "SELECT sf_internal.*, CASE WHEN 1=1 THEN 'Indexable' END AS SF_EXPR_0, "
        "CASE WHEN 1=1 THEN 'Indexable' END AS SF_EXPR_1 "
        "FROM APP.URLS sf_internal WHERE IS_INTERNAL = TRUE"
    )
    assert cursor.executed_params == []
    assert cursor.fetchall_called == 0
    assert len(cursor.fetchmany_calls) >= 1


def test_get_internal_combines_internal_clause_with_filters() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL"],
        [("https://example.com/missing", 404, True)],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._table = "APP.URLS"
    backend._conn = _FakeConnection(cursor)
    backend._column_map = {"status_code": "RESPONSE_CODE"}
    backend._internal_columns = ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL"]
    backend._internal_is_internal_col = "IS_INTERNAL"
    backend._internal_expr_selects = []
    backend._internal_alias_map = {
        "Address": "ENCODED_URL",
        "Status Code": "RESPONSE_CODE",
    }
    backend._internal_header_extract_map = {}

    _ = list(backend.get_internal(filters={"status_code": 404}))

    assert cursor.executed_sql == (
        "SELECT * FROM APP.URLS WHERE IS_INTERNAL = TRUE AND RESPONSE_CODE = ?"
    )
    assert cursor.executed_params == [404]


def test_count_applies_internal_clause_and_filters() -> None:
    cursor = _FakeCursor(["COUNT"], [(7,)])
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._table = "APP.URLS"
    backend._conn = _FakeConnection(cursor)
    backend._column_map = {"status_code": "RESPONSE_CODE"}
    backend._internal_columns = ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL"]
    backend._internal_is_internal_col = "IS_INTERNAL"

    count = backend.count("internal", filters={"status_code": 404})

    assert count == 7
    assert cursor.executed_sql == (
        "SELECT COUNT(*) FROM APP.URLS WHERE IS_INTERNAL = TRUE AND RESPONSE_CODE = ?"
    )
    assert cursor.executed_params == [404]
    assert cursor.fetchone_called == 1


def test_get_tab_skips_null_literal_projection_columns() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL"],
        [("https://example.com/source",)],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "all_inlinks.csv": [
            {
                "csv_column": "Source",
                "db_column": "ENCODED_URL",
                "db_table": "APP.LINKS",
            },
            *[
                {
                    "csv_column": f"Extractor {index}",
                    "db_expression": "NULL",
                    "db_table": "APP.LINKS",
                }
                for index in range(1, 1006)
            ],
        ]
    }

    rows = list(backend.get_tab("all_inlinks"))

    assert len(rows) == 1
    assert rows[0]["Source"] == "https://example.com/source"
    assert rows[0]["Extractor 1"] is None
    assert rows[0]["Extractor 1005"] is None
    assert cursor.executed_sql == "SELECT ENCODED_URL FROM APP.LINKS"


def test_get_tab_materializes_derived_redirect_url_from_meta_and_headers() -> None:
    cursor = _FakeCursor(
        [
            "ENCODED_URL",
            "RESPONSE_CODE",
            "NUM_METAREFRESH",
            "META_FULL_URL_1",
            "META_FULL_URL_2",
            "HTTP_RESPONSE_HEADER_COLLECTION",
        ],
        [
            (
                "https://example.com/source",
                301,
                0,
                None,
                None,
                _headers_blob(Location=["/final"]),
            ),
            (
                "https://example.com/meta",
                200,
                1,
                "/meta-final",
                None,
                None,
            ),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "response_codes_internal_all.csv": [
            {
                "csv_column": "Redirect URL",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "redirect_url",
                    "columns": [
                        "ENCODED_URL",
                        "RESPONSE_CODE",
                        "NUM_METAREFRESH",
                        "META_FULL_URL_1",
                        "META_FULL_URL_2",
                        "HTTP_RESPONSE_HEADER_COLLECTION",
                    ],
                },
            }
        ]
    }

    rows = list(backend.get_tab("response_codes_internal_all"))

    assert rows == [
        {"Redirect URL": "https://example.com/final"},
        {"Redirect URL": "https://example.com/meta-final"},
    ]
    assert cursor.executed_sql == (
        "SELECT ENCODED_URL, RESPONSE_CODE, NUM_METAREFRESH, META_FULL_URL_1, "
        "META_FULL_URL_2, HTTP_RESPONSE_HEADER_COLLECTION FROM APP.URLS"
    )


def test_get_tab_materializes_folder_depth_from_encoded_url() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL"],
        [
            ("https://example.com/",),
            ("https://example.com/section/page.html",),
            ("https://example.com/a/b/",),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Folder Depth",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "folder_depth",
                    "columns": ["ENCODED_URL"],
                },
            }
        ]
    }

    rows = list(backend.get_tab("internal_all"))

    assert rows == [
        {"Folder Depth": 0},
        {"Folder Depth": 1},
        {"Folder Depth": 2},
    ]
    assert cursor.executed_sql == "SELECT ENCODED_URL FROM APP.URLS"


def test_get_tab_materializes_multi_row_custom_extraction_matches() -> None:
    main_cursor = _FakeCursor(
        ["ENCODED_URL"],
        [("https://example.com/page",)],
    )
    extraction_cursor = _FakeCursor(
        ["EXTRACTOR_IDX", "MATCHED"],
        [
            (0, "Alpha"),
            (0, "Beta"),
            (0, "Gamma"),
            (1, "One"),
            (1, "Two"),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _MultiCursorConnection([main_cursor, extraction_cursor])
    backend._mapping = {
        "custom_extraction_all.csv": [
            {
                "csv_column": "Extractor 1 2",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "multi_row_extract": {
                    "type": "custom_extraction_match",
                    "source": "encoded_url",
                    "extractor_idx": 0,
                    "match_index": 2,
                    "columns": ["ENCODED_URL"],
                },
            },
            {
                "csv_column": "Extractor 1 3",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "multi_row_extract": {
                    "type": "custom_extraction_match",
                    "source": "encoded_url",
                    "extractor_idx": 0,
                    "match_index": 3,
                    "columns": ["ENCODED_URL"],
                },
            },
            {
                "csv_column": "Extractor 2 2",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "multi_row_extract": {
                    "type": "custom_extraction_match",
                    "source": "encoded_url",
                    "extractor_idx": 1,
                    "match_index": 2,
                    "columns": ["ENCODED_URL"],
                },
            },
        ]
    }

    rows = list(backend.get_tab("custom_extraction_all"))

    assert rows == [
        {
            "Extractor 1 2": "Beta",
            "Extractor 1 3": "Gamma",
            "Extractor 2 2": "Two",
        }
    ]
    assert main_cursor.executed_sql == "SELECT ENCODED_URL FROM APP.URLS"
    assert extraction_cursor.executed_sql == (
        "SELECT EXTRACTOR_IDX, CAST(MATCHED AS LONG VARCHAR) AS MATCHED "
        "FROM APP.CUSTOM_EXTRACTION WHERE ENCODED_URL = ?"
    )
    assert extraction_cursor.executed_params == ["https://example.com/page"]


def test_get_tab_materializes_multi_row_inlink_custom_extraction_matches() -> None:
    main_cursor = _FakeCursor(
        ["DST_ID"],
        [(42,)],
    )
    id_cursor = _FakeCursor(
        ["ENCODED_URL"],
        [("https://example.com/destination",)],
    )
    extraction_cursor = _FakeCursor(
        ["EXTRACTOR_IDX", "MATCHED"],
        [
            (0, "Alpha"),
            (0, "Beta"),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _MultiCursorConnection([main_cursor, id_cursor, extraction_cursor])
    backend._mapping = {
        "all_inlinks.csv": [
            {
                "csv_column": "Extractor 1 2",
                "db_column": "DST_ID",
                "db_table": "APP.LINKS",
                "multi_row_extract": {
                    "type": "custom_extraction_match",
                    "source": "dst_id",
                    "extractor_idx": 0,
                    "match_index": 2,
                    "columns": ["DST_ID"],
                },
            }
        ]
    }

    rows = list(backend.get_tab("all_inlinks"))

    assert rows == [{"Extractor 1 2": "Beta"}]
    assert main_cursor.executed_sql == "SELECT DST_ID FROM APP.LINKS"
    assert id_cursor.executed_sql == (
        "SELECT ENCODED_URL FROM APP.UNIQUE_URLS WHERE ID = ? FETCH FIRST 1 ROWS ONLY"
    )
    assert id_cursor.executed_params == [42]
    assert extraction_cursor.executed_sql == (
        "SELECT EXTRACTOR_IDX, CAST(MATCHED AS LONG VARCHAR) AS MATCHED "
        "FROM APP.CUSTOM_EXTRACTION WHERE ENCODED_URL = ?"
    )
    assert extraction_cursor.executed_params == ["https://example.com/destination"]


def test_get_tab_extracts_pagespeed_main_thread_work_from_json_blob() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "JSON_RESPONSE"],
        [
            (
                "https://example.com/page",
                _json_blob(
                    {
                        "lighthouseResult": {
                            "audits": {
                                "mainthread-work-breakdown": {
                                    "details": {
                                        "items": [
                                            {"group": "scriptEvaluation", "duration": 111},
                                            {"group": "styleLayout", "duration": 22.5},
                                            {"group": "paintCompositeRender", "duration": 10},
                                            {"group": "paintCompositeRender", "duration": 5},
                                        ]
                                    }
                                }
                            }
                        }
                    }
                ),
            )
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "minimize_main_thread_work_report.csv": [
            {
                "csv_column": "Source Page",
                "db_column": "ENCODED_URL",
                "db_table": "APP.PAGE_SPEED_API",
            },
            {
                "csv_column": "Script Evaluation",
                "db_column": "JSON_RESPONSE",
                "db_table": "APP.PAGE_SPEED_API",
                "blob_extract": {
                    "type": "pagespeed_main_thread_work",
                    "key": "scriptEvaluation",
                },
            },
            {
                "csv_column": "Style & Layout",
                "db_column": "JSON_RESPONSE",
                "db_table": "APP.PAGE_SPEED_API",
                "blob_extract": {
                    "type": "pagespeed_main_thread_work",
                    "key": "styleLayout",
                },
            },
            {
                "csv_column": "Rendering",
                "db_column": "JSON_RESPONSE",
                "db_table": "APP.PAGE_SPEED_API",
                "blob_extract": {
                    "type": "pagespeed_main_thread_work",
                    "key": "paintCompositeRender",
                },
            },
        ]
    }

    rows = list(backend.get_tab("minimize_main_thread_work_report"))

    assert rows == [
        {
            "Source Page": "https://example.com/page",
            "Script Evaluation": 111,
            "Style & Layout": 22.5,
            "Rendering": 15,
        }
    ]
    assert cursor.executed_sql == "SELECT ENCODED_URL, JSON_RESPONSE FROM APP.PAGE_SPEED_API"
