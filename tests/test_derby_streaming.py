from __future__ import annotations

import gzip
import json
from typing import Any

import screamingfrog.backends.derby_backend as derby_backend
from screamingfrog.backends.derby_backend import DerbyBackend, _iter_cursor_rows


class _FakeCursor:
    def __init__(
        self,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        types: list[Any] | None = None,
    ) -> None:
        if types is None:
            self.description = [(col,) for col in columns]
        else:
            self.description = [(col, typ) for col, typ in zip(columns, types)]
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


class _FakeClob:
    def __init__(self, data: str) -> None:
        self._data = data

    def length(self) -> int:
        return len(self._data)

    def getSubString(self, start: int, length: int) -> str:
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


def test_iter_cursor_rows_uses_single_row_batches_for_blob_columns() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "JSON_RESPONSE"],
        [("https://example.com/one", b"one"), ("https://example.com/two", b"two")],
        types=["VARCHAR", "BLOB"],
    )

    rows = list(_iter_cursor_rows(cursor, batch_size=1000))

    assert rows == [
        ("https://example.com/one", b"one"),
        ("https://example.com/two", b"two"),
    ]
    assert cursor.fetchmany_calls == [1, 1, 1]


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


def test_get_internal_batches_overflow_expressions_without_fetchall() -> None:
    main_cursor = _FakeCursor(
        ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL", "SF_EXPR_0"],
        [
            ("https://example.com/", 200, True, "Indexable"),
            ("https://example.com/missing", 404, True, "Non-Indexable"),
        ],
    )
    overflow_cursor = _FakeCursor(
        ["ENCODED_URL", "SF_EXPR_1", "SF_EXPR_2"],
        [
            ("https://example.com/", "https://example.com/canonical", "https://example.com/next"),
            ("https://example.com/missing", None, None),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._table = "APP.URLS"
    backend._conn = _MultiCursorConnection([main_cursor, overflow_cursor])
    backend._column_map = {}
    backend._internal_columns = ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL"]
    backend._internal_is_internal_col = "IS_INTERNAL"
    backend._internal_expr_selects = [
        (
            "SF_EXPR_0",
            "Indexability",
            "CASE WHEN APP.URLS.ENCODED_URL IS NOT NULL THEN 'Indexable' END",
        ),
        (
            "SF_EXPR_1",
            "Canonical Link Element 1",
            "CASE WHEN APP.URLS.ENCODED_URL IS NOT NULL THEN '/canonical' END",
        ),
        (
            "SF_EXPR_2",
            "Rel Next 1",
            "CASE WHEN APP.URLS.ENCODED_URL IS NOT NULL THEN '/next' END",
        ),
    ]
    backend._internal_alias_map = {
        "Address": "ENCODED_URL",
        "Status Code": "RESPONSE_CODE",
    }
    backend._internal_header_extract_map = {}
    backend._DERBY_SELECT_LIMIT = 4
    backend._INTERNAL_OVERFLOW_BATCH_SIZE = 10

    pages = list(backend.get_internal())

    assert [page.address for page in pages] == [
        "https://example.com/",
        "https://example.com/missing",
    ]
    assert pages[0].data["Indexability"] == "Indexable"
    assert pages[0].data["Canonical Link Element 1"] == "https://example.com/canonical"
    assert pages[0].data["Rel Next 1"] == "https://example.com/next"
    assert pages[1].data["Canonical Link Element 1"] is None
    assert pages[1].data["Rel Next 1"] is None
    assert main_cursor.fetchall_called == 0
    assert overflow_cursor.fetchall_called == 0
    assert main_cursor.executed_sql == (
        "SELECT sf_internal.*, CASE WHEN sf_internal.ENCODED_URL IS NOT NULL THEN 'Indexable' END AS SF_EXPR_0 "
        "FROM APP.URLS sf_internal WHERE IS_INTERNAL = TRUE"
    )
    assert overflow_cursor.executed_sql == (
        "SELECT sf_internal.ENCODED_URL, "
        "CASE WHEN sf_internal.ENCODED_URL IS NOT NULL THEN '/canonical' END AS SF_EXPR_1, "
        "CASE WHEN sf_internal.ENCODED_URL IS NOT NULL THEN '/next' END AS SF_EXPR_2 "
        "FROM APP.URLS sf_internal WHERE IS_INTERNAL = TRUE "
        "AND sf_internal.ENCODED_URL IN (?, ?)"
    )
    assert overflow_cursor.executed_params == [
        "https://example.com/",
        "https://example.com/missing",
    ]
    assert len(overflow_cursor.fetchmany_calls) >= 1


def test_get_internal_overflow_batches_stay_bounded_during_partial_iteration() -> None:
    main_cursor = _FakeCursor(
        ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL", "ORIGINAL_CONTENT", "SF_EXPR_0"],
        [
            ("https://example.com/one", 200, True, _FakeClob("<html>1</html>"), "Indexable"),
            ("https://example.com/two", 200, True, _FakeClob("<html>2</html>"), "Indexable"),
            ("https://example.com/three", 200, True, _FakeClob("<html>3</html>"), "Indexable"),
        ],
        types=["VARCHAR", "INTEGER", "BOOLEAN", "CLOB", "VARCHAR"],
    )
    overflow_cursor_one = _FakeCursor(
        ["ENCODED_URL", "SF_EXPR_1"],
        [
            ("https://example.com/one", "alpha"),
            ("https://example.com/two", "beta"),
        ],
    )
    overflow_cursor_two = _FakeCursor(
        ["ENCODED_URL", "SF_EXPR_1"],
        [("https://example.com/three", "gamma")],
    )
    connection = _MultiCursorConnection([main_cursor, overflow_cursor_one, overflow_cursor_two])
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._table = "APP.URLS"
    backend._conn = connection
    backend._column_map = {}
    backend._internal_columns = [
        "ENCODED_URL",
        "RESPONSE_CODE",
        "IS_INTERNAL",
        "ORIGINAL_CONTENT",
    ]
    backend._internal_is_internal_col = "IS_INTERNAL"
    backend._internal_expr_selects = [
        (
            "SF_EXPR_0",
            "Indexability",
            "CASE WHEN APP.URLS.ENCODED_URL IS NOT NULL THEN 'Indexable' END",
        ),
        (
            "SF_EXPR_1",
            "Canonical Link Element 1",
            "CASE WHEN APP.URLS.ENCODED_URL IS NOT NULL THEN '/canonical' END",
        ),
    ]
    backend._internal_alias_map = {"Address": "ENCODED_URL"}
    backend._internal_header_extract_map = {}
    backend._DERBY_SELECT_LIMIT = 5
    backend._INTERNAL_OVERFLOW_BATCH_SIZE = 2

    iterator = backend.get_internal()
    first_page = next(iterator)

    assert first_page.address == "https://example.com/one"
    assert first_page.data["Canonical Link Element 1"] == "alpha"
    assert connection._index == 2
    assert overflow_cursor_one.executed_params == [
        "https://example.com/one",
        "https://example.com/two",
    ]
    assert overflow_cursor_two.executed_sql is None
    assert overflow_cursor_one.fetchall_called == 0
    assert overflow_cursor_two.fetchall_called == 0
    assert main_cursor.fetchmany_calls[:2] == [1, 1]

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


def test_get_tab_materializes_pixel_widths_and_carbon_rating(monkeypatch: Any) -> None:
    cursor = _FakeCursor(
        ["TITLE_1", "META_NAME_1", "META_CONTENT_1", "CO2"],
        [
            ("Short title", "description", "Description copy", 0.0),
            ("Another title", "description", "Another description", 269.914),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Title 1 Pixel Width",
                "db_column": "TITLE_1",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "pixel_width",
                    "profile": "title",
                    "columns": ["TITLE_1"],
                },
            },
            {
                "csv_column": "Meta Description 1 Pixel Width",
                "db_column": "META_NAME_1",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "meta_description_pixel_width",
                    "columns": ["META_NAME_1", "META_CONTENT_1"],
                },
            },
            {
                "csv_column": "Carbon Rating",
                "db_column": "CO2",
                "db_table": "APP.URLS",
                "derived_extract": {"type": "carbon_rating", "columns": ["CO2"]},
            },
        ]
    }

    def fake_measure(text: str, *, family: str, size: int, weight: str) -> int:
        return {"Short title": 100, "Description copy": 200, "Another title": 110, "Another description": 220}[text]

    monkeypatch.setattr(derby_backend, "_measure_text_pixels_tk", fake_measure)

    rows = list(backend.get_tab("internal_all"))

    assert rows == [
        {
            "Title 1 Pixel Width": 105,
            "Meta Description 1 Pixel Width": 195,
            "Carbon Rating": "A+",
        },
        {
            "Title 1 Pixel Width": 116,
            "Meta Description 1 Pixel Width": 215,
            "Carbon Rating": "B",
        },
    ]
    assert cursor.executed_sql == "SELECT TITLE_1, META_NAME_1, META_CONTENT_1, CO2 FROM APP.URLS"


def test_get_structured_data_detail_tabs_filter_formats_and_validation_counts(
    monkeypatch: Any,
) -> None:
    payload = {
        "inspectionResult": {
            "richResultsResult": {
                "detectedItems": [
                    {
                        "richResultType": "FAQ",
                        "items": [
                            {
                                "name": "FAQPage",
                                "issues": [
                                    {"severity": "ERROR", "issueMessage": "Missing field"},
                                    {"severity": "WARNING", "issueMessage": "Recommended field"},
                                ],
                            }
                        ],
                    }
                ]
            }
        }
    }
    cursor = _FakeCursor(
        [
            "ENCODED_URL",
            "SERIALISED_STRUCTURED_DATA",
            "RICH_RESULTS_TYPE_ERRORS",
            "RICH_RESULTS_TYPE_WARNINGS",
            "JSON",
        ],
        [
            ("https://example.com/jsonld", b"blob-a", 2, 1, _json_blob(payload)),
            ("https://example.com/rdfa", b"blob-b", 0, 3, _json_blob(payload)),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "jsonld_urls_detailed_report.csv": [
            {"csv_column": "URL", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Subject", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Subject"}},
            {"csv_column": "Predicate", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Predicate"}},
            {"csv_column": "Object", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Object"}},
            {"csv_column": "Errors", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Errors"}},
            {"csv_column": "Warnings", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Warnings"}},
            {"csv_column": "Validation Type 1", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Validation Type 1"}},
            {"csv_column": "Severity 1", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Severity 1"}},
            {"csv_column": "Issue 1", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Issue 1"}},
            {"csv_column": "Validation Type 2", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Validation Type 2"}},
            {"csv_column": "Severity 2", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Severity 2"}},
            {"csv_column": "Issue 2", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Issue 2"}},
        ],
        "validation_errors_detailed_report.csv": [
            {"csv_column": "URL", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Subject", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Subject"}},
            {"csv_column": "Predicate", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Predicate"}},
            {"csv_column": "Object", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Object"}},
            {"csv_column": "Errors", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Errors"}},
            {"csv_column": "Warnings", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Warnings"}},
            {"csv_column": "Validation Type 1", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Validation Type 1"}},
            {"csv_column": "Severity 1", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Severity 1"}},
            {"csv_column": "Issue 1", "db_expression": "NULL", "db_table": "APP.URLS", "runtime_extract": {"type": "structured_data_detailed", "field": "Issue 1"}},
        ],
    }

    def fake_blocks(blob: bytes) -> list[dict[str, Any]]:
        if blob == b"blob-a":
            return [{"format": "JSONLD", "text": "jsonld"}]
        return [{"format": "RDFA", "text": "rdfa"}]

    monkeypatch.setattr(derby_backend, "_parse_structured_data_blocks", fake_blocks)
    monkeypatch.setattr(
        derby_backend,
        "_iter_structured_data_triples",
        lambda text: iter([("subject0", "https://schema.org/name", "_:node1")]),
    )

    jsonld_rows = list(backend.get_tab("jsonld_urls_detailed_report"))
    cursor._index = 0
    cursor._fetchone_index = 0
    error_rows = list(backend.get_tab("validation_errors_detailed_report"))

    assert jsonld_rows == [
        {
            "URL": "https://example.com/jsonld",
            "Subject": "subject0",
            "Predicate": "https://schema.org/name",
            "Object": "subject1",
            "Errors": 2,
            "Warnings": 1,
            "Validation Type 1": "FAQ",
            "Severity 1": "ERROR",
            "Issue 1": "Missing field",
            "Validation Type 2": "FAQ",
            "Severity 2": "WARNING",
            "Issue 2": "Recommended field",
        }
    ]
    assert error_rows == [
        {
            "URL": "https://example.com/jsonld",
            "Subject": "subject0",
            "Predicate": "https://schema.org/name",
            "Object": "subject1",
            "Errors": 2,
            "Warnings": 1,
            "Validation Type 1": "FAQ",
            "Severity 1": "ERROR",
            "Issue 1": "Missing field",
        }
    ]


def test_get_http_header_summary_tab_collects_unique_request_header_names() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "HTTP_REQUEST_HEADER_COLLECTION"],
        [
            (
                "https://example.com/one",
                _headers_blob(**{"accept": ["*/*"], "user-agent": ["UA"]}),
            ),
            (
                "https://example.com/two",
                _headers_blob(**{"accept-encoding": ["gzip"], "accept": ["text/html"]}),
            ),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "http_header_summary.csv": [
            {
                "csv_column": "HTTP Request Headers",
                "db_expression": "NULL",
                "db_table": "APP.URLS",
                "runtime_extract": {
                    "type": "http_header_summary",
                    "field": "HTTP Request Headers",
                },
            }
        ]
    }

    rows = list(backend.get_tab("http_header_summary"))

    assert rows == [
        {"HTTP Request Headers": "Accept"},
        {"HTTP Request Headers": "Accept-Encoding"},
        {"HTTP Request Headers": "User-Agent"},
    ]


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


def test_get_tab_fetches_chrome_console_supplementary_columns_by_encoded_url() -> None:
    main_cursor = _FakeCursor(
        ["ENCODED_URL", "ENCODED_URL", "TITLE_1"],
        [
            (
                "https://example.com/page",
                "https://example.com/page",
                "Base Title",
            )
        ],
    )
    console_cursor = _FakeCursor(["NUM_ERRORS"], [(5,)])

    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _MultiCursorConnection([main_cursor, console_cursor])
    backend._mapping = {
        "javascript_all.csv": [
            {
                "csv_column": "Address",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "HTML Title",
                "db_column": "TITLE_1",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "JS Error",
                "db_column": "NUM_ERRORS",
                "db_table": "APP.CHROME_CONSOLE_DATA",
            },
        ]
    }

    rows = list(backend.get_tab("javascript_all"))

    assert rows == [
        {
            "Address": "https://example.com/page",
            "HTML Title": "Base Title",
            "JS Error": 5,
        }
    ]
    assert main_cursor.executed_sql == "SELECT ENCODED_URL, ENCODED_URL, TITLE_1 FROM APP.URLS"
    assert console_cursor.executed_sql == (
        "SELECT NUM_ERRORS FROM APP.CHROME_CONSOLE_DATA "
        "WHERE ENCODED_URL = ? FETCH FIRST 1 ROWS ONLY"
    )
    assert console_cursor.executed_params == ["https://example.com/page"]


def test_get_accessibility_summary_tab_parses_axe_results() -> None:
    axe_payload_one = {
        "violations": [
            {
                "id": "image-alt",
                "impact": "critical",
                "description": "Ensure images have alternate text",
                "help": "Images Require Alternate Text",
                "helpUrl": "https://example.com/image-alt",
                "tags": ["wcag2a"],
                "nodes": [{"target": [".hero > img"]}],
            },
            {
                "id": "landmark-one-main",
                "impact": "moderate",
                "description": "Ensure one main landmark exists",
                "help": "Page Requires One Main Landmark",
                "helpUrl": "https://example.com/landmark",
                "tags": ["best-practice"],
                "nodes": [{"target": ["main"]}],
            },
        ]
    }
    axe_payload_two = {
        "violations": [
            {
                "id": "image-alt",
                "impact": "critical",
                "description": "Ensure images have alternate text",
                "help": "Images Require Alternate Text",
                "helpUrl": "https://example.com/image-alt",
                "tags": ["wcag2a"],
                "nodes": [{"target": [".promo > img"]}],
            }
        ]
    }
    cursor = _FakeCursor(
        ["ENCODED_URL", "COMPRESSED_JSON"],
        [
            ("https://example.com/one", _json_blob(axe_payload_one)),
            ("https://example.com/two", _json_blob(axe_payload_two)),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "accessibility_violations_summary.csv": [
            {"csv_column": "Issue", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "Issue"}},
            {"csv_column": "Guidelines", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "Guidelines"}},
            {"csv_column": "User Impact", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "User Impact"}},
            {"csv_column": "Priority", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "Priority"}},
            {"csv_column": "Total URLs Crawled", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "Total URLs Crawled"}},
            {"csv_column": "Number of URLs with Violations", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "Number of URLs with Violations"}},
            {"csv_column": "% URLs in Violation", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "% URLs in Violation"}},
            {"csv_column": "Sample Affected URL", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_summary", "field": "Sample Affected URL"}},
        ]
    }

    rows = list(backend.get_tab("accessibility_violations_summary"))

    assert rows == [
        {
            "Issue": "Images Require Alternate Text",
            "Guidelines": "WCAG 2.0 A",
            "User Impact": "Critical",
            "Priority": "High",
            "Total URLs Crawled": 2,
            "Number of URLs with Violations": 2,
            "% URLs in Violation": 100.0,
            "Sample Affected URL": "https://example.com/one",
        },
        {
            "Issue": "Page Requires One Main Landmark",
            "Guidelines": "Best Practice",
            "User Impact": "Moderate",
            "Priority": "Medium",
            "Total URLs Crawled": 2,
            "Number of URLs with Violations": 1,
            "% URLs in Violation": 50.0,
            "Sample Affected URL": "https://example.com/one",
        },
    ]


def test_get_accessibility_detail_tab_filters_issue_group_and_node_location() -> None:
    axe_payload = {
        "violations": [
            {
                "id": "image-alt",
                "impact": "critical",
                "description": "Ensure images have alternate text",
                "help": "Images Require Alternate Text",
                "helpUrl": "https://example.com/image-alt",
                "tags": ["wcag2a"],
                "nodes": [{"target": [".hero > img"]}],
            },
            {
                "id": "landmark-one-main",
                "impact": "moderate",
                "description": "Ensure one main landmark exists",
                "help": "Page Requires One Main Landmark",
                "helpUrl": "https://example.com/landmark",
                "tags": ["best-practice"],
                "nodes": [{"target": ["main"]}],
            },
        ],
        "incomplete": [
            {
                "id": "color-contrast",
                "impact": "serious",
                "description": "Ensure contrast is sufficient",
                "help": "Elements Must Meet Minimum Color Contrast Ratio Thresholds",
                "helpUrl": "https://example.com/contrast",
                "tags": ["wcag2aa"],
                "nodes": [{"target": ["#cta > a"]}],
            }
        ],
    }
    cursor = _FakeCursor(
        ["ENCODED_URL", "COMPRESSED_JSON"],
        [("https://example.com/page", _json_blob(axe_payload))],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "best_practice_all_violations.csv": [
            {"csv_column": "Issue", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "Issue"}},
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Location on Page", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "Location on Page"}},
            {"csv_column": "Guidelines", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "Guidelines"}},
            {"csv_column": "User Impact", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "User Impact"}},
            {"csv_column": "Priority", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "Priority"}},
            {"csv_column": "Issue Description", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "Issue Description"}},
            {"csv_column": "How To Fix", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "How To Fix"}},
            {"csv_column": "Help URL", "db_expression": "NULL", "runtime_extract": {"type": "accessibility_detail", "field": "Help URL"}},
        ]
    }

    rows = list(backend.get_tab("best_practice_all_violations"))

    assert rows == [
        {
            "Issue": "Page Requires One Main Landmark",
            "Address": "https://example.com/page",
            "Location on Page": "main",
            "Guidelines": "Best Practice",
            "User Impact": "Moderate",
            "Priority": "Medium",
            "Issue Description": "Page Requires One Main Landmark.",
            "How To Fix": "Ensure one main landmark exists.",
            "Help URL": "https://example.com/landmark",
        }
    ]


def test_get_pagespeed_coverage_summary_parses_lighthouse_audits() -> None:
    payload_one = {
        "lighthouseResult": {
            "audits": {
                "unused-css-rules": {
                    "details": {
                        "overallSavingsMs": 100,
                        "items": [
                            {
                                "url": "https://cdn.example.com/app.css",
                                "totalBytes": 100,
                                "wastedBytes": 50,
                            }
                        ]
                    }
                }
            }
        }
    }
    payload_two = {
        "lighthouseResult": {
            "audits": {
                "unused-css-rules": {
                    "details": {
                        "overallSavingsMs": 100,
                        "items": [
                            {
                                "url": "https://cdn.example.com/app.css",
                                "totalBytes": 120,
                                "wastedBytes": 60,
                            }
                        ]
                    }
                }
            }
        }
    }
    cursor = _FakeCursor(
        ["ENCODED_URL", "JSON_RESPONSE"],
        [
            ("https://example.com/one", _json_blob(payload_one)),
            ("https://example.com/two", _json_blob(payload_two)),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "css_coverage_summary.csv": [
            {"csv_column": "Resource", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_coverage_summary", "field": "Resource"}},
            {"csv_column": "Total Bytes", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_coverage_summary", "field": "Total Bytes"}},
            {"csv_column": "Average Unused Bytes", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_coverage_summary", "field": "Average Unused Bytes"}},
            {"csv_column": "Average Unused Percentage", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_coverage_summary", "field": "Average Unused Percentage"}},
            {"csv_column": "Affected URLs", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_coverage_summary", "field": "Affected URLs"}},
            {"csv_column": "Unused URLs", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_coverage_summary", "field": "Unused URLs"}},
        ]
    }

    rows = list(backend.get_tab("css_coverage_summary"))

    assert rows == [
        {
            "Resource": "https://cdn.example.com/app.css",
            "Total Bytes": 110,
            "Average Unused Bytes": 55,
            "Average Unused Percentage": 50.0,
            "Affected URLs": 2,
            "Unused URLs": 0,
        }
    ]


def test_get_pagespeed_opportunity_summary_aggregates_by_audit_label() -> None:
    payload_one = {
        "lighthouseResult": {
            "audits": {
                "unused-javascript": {
                    "details": {
                        "overallSavingsMs": 500,
                        "overallSavingsBytes": 1000,
                        "items": [{"url": "https://cdn.example.com/app.js", "totalBytes": 2000}],
                    }
                }
            }
        }
    }
    payload_two = {
        "lighthouseResult": {
            "audits": {
                "unused-javascript": {
                    "details": {
                        "overallSavingsMs": 0,
                        "overallSavingsBytes": 0,
                        "items": [{"url": "https://cdn.example.com/app.js", "totalBytes": 3000}],
                    }
                }
            }
        }
    }
    cursor = _FakeCursor(
        ["ENCODED_URL", "JSON_RESPONSE"],
        [
            ("https://example.com/one", _json_blob(payload_one)),
            ("https://example.com/two", _json_blob(payload_two)),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "pagespeed_opportunities_summary.csv": [
            {"csv_column": "Opportunity", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Opportunity"}},
            {"csv_column": "Number of URLs Affected", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Number of URLs Affected"}},
            {"csv_column": "Total Size Bytes", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Total Size Bytes"}},
            {"csv_column": "Total Savings ms", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Total Savings ms"}},
            {"csv_column": "Total Savings Size Bytes", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Total Savings Size Bytes"}},
            {"csv_column": "Average Savings ms", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Average Savings ms"}},
            {"csv_column": "Average Savings Size Bytes", "db_expression": "NULL", "runtime_extract": {"type": "pagespeed_opportunity_summary", "field": "Average Savings Size Bytes"}},
        ]
    }

    rows = list(backend.get_tab("pagespeed_opportunities_summary"))
    javascript_row = next(row for row in rows if row["Opportunity"] == "Reduce Unused JavaScript")
    css_row = next(row for row in rows if row["Opportunity"] == "Reduce Unused CSS")

    assert javascript_row == {
        "Opportunity": "Reduce Unused JavaScript",
        "Number of URLs Affected": 1,
        "Total Size Bytes": 2000,
        "Total Savings ms": 500,
        "Total Savings Size Bytes": 1000,
        "Average Savings ms": 500,
        "Average Savings Size Bytes": 1000,
    }
    assert css_row == {
        "Opportunity": "Reduce Unused CSS",
        "Number of URLs Affected": 0,
        "Total Size Bytes": 0,
        "Total Savings ms": 0,
        "Total Savings Size Bytes": 0,
        "Average Savings ms": 0,
        "Average Savings Size Bytes": 0,
    }


def test_get_pagespeed_detail_tabs_parse_lighthouse_items() -> None:
    payload = {
        "lighthouseResult": {
            "audits": {
                "dom-size": {
                    "details": {
                        "items": [
                            {
                                "statistic": "Maximum DOM Depth",
                                "selector": "main .card",
                                "snippet": '<div class="card">',
                                "value": 42,
                            }
                        ]
                    }
                },
                "layout-shifts": {
                    "details": {
                        "items": [
                            {
                                "node": {
                                    "nodeLabel": "Hero banner",
                                    "snippet": '<section class="hero">',
                                    "selector": "section.hero",
                                },
                                "score": 0.188,
                            }
                        ]
                    }
                },
                "legacy-javascript": {
                    "details": {
                        "items": [
                            {
                                "url": "https://cdn.example.com/legacy.js",
                                "totalBytes": 2048,
                                "wastedBytes": 512,
                            }
                        ]
                    }
                },
                "bootup-time": {
                    "details": {
                        "items": [
                            {
                                "url": "https://cdn.example.com/app.js",
                                "total": 120.5,
                                "scripting": 80.25,
                                "scriptParseCompile": 20.0,
                            }
                        ]
                    }
                },
                "uses-long-cache-ttl": {
                    "details": {
                        "items": [
                            {
                                "url": "https://cdn.example.com/cache.js",
                                "cacheLifetimeMs": 86400000,
                                "totalBytes": 4096,
                            }
                        ]
                    }
                },
                "font-size": {
                    "details": {
                        "totalTextLength": 200,
                        "items": [
                            {
                                "fontSize": 10,
                                "textLength": 50,
                                "selector": "p.small",
                                "url": "https://example.com/page",
                            }
                        ],
                    }
                },
                "unsized-images": {
                    "details": {
                        "items": [
                            {
                                "url": "https://cdn.example.com/image.png",
                                "node": {
                                    "nodeLabel": "Hero image",
                                    "snippet": '<img src="https://cdn.example.com/image.png">',
                                },
                            }
                        ]
                    }
                },
                "offscreen-images": {
                    "details": {
                        "items": [
                            {
                                "url": "https://cdn.example.com/offscreen.png",
                                "totalBytes": 1234,
                                "wastedBytes": 456,
                            }
                        ]
                    }
                },
                "efficient-animated-content": {
                    "details": {
                        "items": [
                            {
                                "url": "https://cdn.example.com/anim.gif",
                                "totalBytes": 8000,
                                "wastedBytes": 7000,
                            }
                        ]
                    }
                },
            }
        }
    }
    rows = [("https://example.com/page", _json_blob(payload))]
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _MultiCursorConnection(
        [
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
            _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], rows),
        ]
    )
    runtime = lambda tab, field: {
        "csv_column": field,
        "db_expression": "NULL",
        "runtime_extract": {"type": "pagespeed_detail", "tab": tab, "field": field},
    }
    backend._mapping = {
        "avoid_excessive_dom_size_report.csv": [
            runtime("avoid_excessive_dom_size_report.csv", "URL"),
            runtime("avoid_excessive_dom_size_report.csv", "Statistic"),
            runtime("avoid_excessive_dom_size_report.csv", "Selector"),
            runtime("avoid_excessive_dom_size_report.csv", "Snippet"),
            runtime("avoid_excessive_dom_size_report.csv", "Value"),
        ],
        "avoid_large_layout_shifts_report.csv": [
            runtime("avoid_large_layout_shifts_report.csv", "Source Page"),
            runtime("avoid_large_layout_shifts_report.csv", "Label"),
            runtime("avoid_large_layout_shifts_report.csv", "Snippet"),
            runtime("avoid_large_layout_shifts_report.csv", "CLS Contribution"),
        ],
        "avoid_serving_legacy_javascript_to_modern_browsers_report.csv": [
            runtime("avoid_serving_legacy_javascript_to_modern_browsers_report.csv", "Source Page"),
            runtime("avoid_serving_legacy_javascript_to_modern_browsers_report.csv", "URL"),
            runtime("avoid_serving_legacy_javascript_to_modern_browsers_report.csv", "Size (Bytes)"),
            runtime("avoid_serving_legacy_javascript_to_modern_browsers_report.csv", "Potential Savings (Bytes)"),
        ],
        "reduce_javascript_execution_time_report.csv": [
            runtime("reduce_javascript_execution_time_report.csv", "Source Page"),
            runtime("reduce_javascript_execution_time_report.csv", "URL"),
            runtime("reduce_javascript_execution_time_report.csv", "Total CPU Time (ms)"),
            runtime("reduce_javascript_execution_time_report.csv", "Script Evaluation"),
            runtime("reduce_javascript_execution_time_report.csv", "Script Parse"),
        ],
        "serve_static_assets_with_an_efficient_cache_policy_report.csv": [
            runtime("serve_static_assets_with_an_efficient_cache_policy_report.csv", "Source Page"),
            runtime("serve_static_assets_with_an_efficient_cache_policy_report.csv", "URL"),
            runtime("serve_static_assets_with_an_efficient_cache_policy_report.csv", "Cache TTL (ms)"),
            runtime("serve_static_assets_with_an_efficient_cache_policy_report.csv", "Size (Bytes)"),
        ],
        "illegible_font_size_report.csv": [
            runtime("illegible_font_size_report.csv", "Source Page"),
            runtime("illegible_font_size_report.csv", "Font Size"),
            runtime("illegible_font_size_report.csv", "% of Page Text"),
            runtime("illegible_font_size_report.csv", "Selector"),
            runtime("illegible_font_size_report.csv", "URL"),
        ],
        "image_elements_do_not_have_explicit_width_&_height_report.csv": [
            runtime("image_elements_do_not_have_explicit_width_&_height_report.csv", "Source Page"),
            runtime("image_elements_do_not_have_explicit_width_&_height_report.csv", "URL"),
            runtime("image_elements_do_not_have_explicit_width_&_height_report.csv", "Label"),
            runtime("image_elements_do_not_have_explicit_width_&_height_report.csv", "Snippet"),
        ],
        "defer_offscreen_images_report.csv": [
            runtime("defer_offscreen_images_report.csv", "Source Page"),
            runtime("defer_offscreen_images_report.csv", "Image URL"),
            runtime("defer_offscreen_images_report.csv", "Size (Bytes)"),
            runtime("defer_offscreen_images_report.csv", "Potential Savings (Bytes)"),
        ],
        "use_video_formats_for_animated_content_report.csv": [
            runtime("use_video_formats_for_animated_content_report.csv", "Source Page"),
            runtime("use_video_formats_for_animated_content_report.csv", "Image URL"),
            runtime("use_video_formats_for_animated_content_report.csv", "Size (Bytes)"),
            runtime("use_video_formats_for_animated_content_report.csv", "Potential Savings (Bytes)"),
        ],
    }

    assert list(backend.get_tab("avoid_excessive_dom_size_report")) == [
        {
            "URL": "https://example.com/page",
            "Statistic": "Maximum DOM Depth",
            "Selector": "main .card",
            "Snippet": '<div class="card">',
            "Value": 42,
        }
    ]
    assert list(backend.get_tab("avoid_large_layout_shifts_report")) == [
        {
            "Source Page": "https://example.com/page",
            "Label": "Hero banner",
            "Snippet": '<section class="hero">',
            "CLS Contribution": 0.188,
        }
    ]
    assert list(backend.get_tab("avoid_serving_legacy_javascript_to_modern_browsers_report")) == [
        {
            "Source Page": "https://example.com/page",
            "URL": "https://cdn.example.com/legacy.js",
            "Size (Bytes)": 2048,
            "Potential Savings (Bytes)": 512,
        }
    ]
    assert list(backend.get_tab("reduce_javascript_execution_time_report")) == [
        {
            "Source Page": "https://example.com/page",
            "URL": "https://cdn.example.com/app.js",
            "Total CPU Time (ms)": 120.5,
            "Script Evaluation": 80.25,
            "Script Parse": 20.0,
        }
    ]
    assert list(backend.get_tab("serve_static_assets_with_an_efficient_cache_policy_report")) == [
        {
            "Source Page": "https://example.com/page",
            "URL": "https://cdn.example.com/cache.js",
            "Cache TTL (ms)": 86400000,
            "Size (Bytes)": 4096,
        }
    ]
    assert list(backend.get_tab("illegible_font_size_report")) == [
        {
            "Source Page": "https://example.com/page",
            "Font Size": 10.0,
            "% of Page Text": 25.0,
            "Selector": "p.small",
            "URL": "https://example.com/page",
        }
    ]
    assert list(backend.get_tab("image_elements_do_not_have_explicit_width_&_height_report")) == [
        {
            "Source Page": "https://example.com/page",
            "URL": "https://cdn.example.com/image.png",
            "Label": "Hero image",
            "Snippet": '<img src="https://cdn.example.com/image.png">',
        }
    ]
    assert list(backend.get_tab("defer_offscreen_images_report")) == [
        {
            "Source Page": "https://example.com/page",
            "Image URL": "https://cdn.example.com/offscreen.png",
            "Size (Bytes)": 1234,
            "Potential Savings (Bytes)": 456,
        }
    ]
    assert list(backend.get_tab("use_video_formats_for_animated_content_report")) == [
        {
            "Source Page": "https://example.com/page",
            "Image URL": "https://cdn.example.com/anim.gif",
            "Size (Bytes)": 8000,
            "Potential Savings (Bytes)": 7000,
        }
    ]


def test_get_google_rich_results_features_report_marks_detected_columns() -> None:
    payload = {
        "inspectionResult": {
            "richResultsResult": {
                "detectedItems": [
                    {
                        "richResultType": "FAQ",
                        "items": [
                            {
                                "issues": [
                                    {"severity": "ERROR", "issueMessage": "Missing field"}
                                ]
                            }
                        ],
                    },
                    {"richResultType": "Article", "items": [{}]},
                ]
            }
        }
    }
    cursor = _FakeCursor(
        ["ENCODED_URL", "RICH_RESULTS_TYPES", "RICH_RESULTS_TYPE_ERRORS", "RICH_RESULTS_TYPE_WARNINGS", "JSON"],
        [("https://example.com/page", None, 1, 0, _json_blob(payload))],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "google_rich_results_features_report.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Google FAQ", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_report", "field": "Google FAQ"}},
            {"csv_column": "Google Article", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_report", "field": "Google Article"}},
            {"csv_column": "Google Breadcrumb", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_report", "field": "Google Breadcrumb"}},
        ]
    }

    rows = list(backend.get_tab("google_rich_results_features_report"))

    assert rows == [
        {
            "Address": "https://example.com/page",
            "Google FAQ": "detected",
            "Google Article": "detected",
            "Google Breadcrumb": None,
        }
    ]


def test_get_google_rich_results_features_summary_report_aggregates_feature_metrics() -> None:
    payload = {
        "inspectionResult": {
            "richResultsResult": {
                "detectedItems": [
                    {
                        "richResultType": "FAQ",
                        "items": [
                            {"issues": [{"severity": "ERROR", "issueMessage": "Missing field"}]},
                            {"issues": [{"severity": "WARNING", "issueMessage": "Recommended field"}]},
                        ],
                    }
                ]
            }
        }
    }
    fallback_payload = {}
    cursor = _FakeCursor(
        ["ENCODED_URL", "RICH_RESULTS_TYPES", "RICH_RESULTS_TYPE_ERRORS", "RICH_RESULTS_TYPE_WARNINGS", "JSON"],
        [
            ("https://example.com/one", None, 0, 0, _json_blob(payload)),
            ("https://example.com/two", "FAQ", 0, 0, _json_blob(fallback_payload)),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "google_rich_results_features_report.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Google FAQ", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_report", "field": "Google FAQ"}},
            {"csv_column": "Google Article", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_report", "field": "Google Article"}},
        ],
        "google_rich_results_features_summary_report.csv": [
            {"csv_column": "Rich Results Feature", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Rich Results Feature"}},
            {"csv_column": "URLs", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "URLs"}},
            {"csv_column": "Occurrences", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Occurrences"}},
            {"csv_column": "% Eligible", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "% Eligible"}},
            {"csv_column": "Eligible URLs", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Eligible URLs"}},
            {"csv_column": "Error URLs", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Error URLs"}},
            {"csv_column": "Warning URLs", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Warning URLs"}},
            {"csv_column": "Unique Errors", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Unique Errors"}},
            {"csv_column": "Unique Warnings", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Unique Warnings"}},
            {"csv_column": "Total Errors", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Total Errors"}},
            {"csv_column": "Total Warnings", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Total Warnings"}},
            {"csv_column": "Sample URL", "db_expression": "NULL", "runtime_extract": {"type": "google_rich_results_features_summary", "field": "Sample URL"}},
        ],
    }

    rows = list(backend.get_tab("google_rich_results_features_summary_report"))

    assert rows == [
        {
            "Rich Results Feature": "Google FAQ",
            "URLs": 2,
            "Occurrences": 3,
            "% Eligible": 50,
            "Eligible URLs": 1,
            "Error URLs": 1,
            "Warning URLs": 1,
            "Unique Errors": 1,
            "Unique Warnings": 1,
            "Total Errors": 1,
            "Total Warnings": 1,
            "Sample URL": "https://example.com/one",
        }
    ]


def test_get_url_inspection_rich_results_extracts_first_issue_fields() -> None:
    payload = {
        "inspectionResult": {
            "richResultsResult": {
                "detectedItems": [
                    {
                        "richResultType": "FAQ",
                        "items": [
                            {
                                "name": "FAQPage",
                                "issues": [
                                    {
                                        "severity": "ERROR",
                                        "issueMessage": "Missing field",
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        }
    }
    main_cursor = _FakeCursor(
        [
            "ENCODED_URL",
            "RICH_RESULTS_VERDICT",
            "RICH_RESULTS_TYPES",
            "RICH_RESULTS_TYPE_ERRORS",
            "RICH_RESULTS_TYPE_WARNINGS",
            "JSON",
        ],
        [("https://example.com/page", "PASS", "FAQ", 1, 0, _json_blob(payload))],
    )
    indexability_cursor = _FakeCursor(
        ["INDEXABILITY", "INDEXABILITY_STATUS"],
        [("Indexable", "Indexable")],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _MultiCursorConnection([main_cursor, indexability_cursor])
    backend._mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Indexability",
                "db_expression": "'Indexable'",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Indexability Status",
                "db_expression": "'Indexable'",
                "db_table": "APP.URLS",
            },
        ],
        "url_inspection_rich_results.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URL_INSPECTION"},
            {"csv_column": "Indexability", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Indexability"}},
            {"csv_column": "Indexability Status", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Indexability Status"}},
            {"csv_column": "Rich Results", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Rich Results"}},
            {"csv_column": "Rich Results Type", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Rich Results Type"}},
            {"csv_column": "Severity", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Severity"}},
            {"csv_column": "Item Name", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Item Name"}},
            {"csv_column": "Rich Results Issue Type", "db_expression": "NULL", "runtime_extract": {"type": "url_inspection_rich_results", "field": "Rich Results Issue Type"}},
        ],
    }

    rows = list(backend.get_tab("url_inspection_rich_results"))

    assert rows == [
        {
            "Address": "https://example.com/page",
            "Indexability": "Indexable",
            "Indexability Status": "Indexable",
            "Rich Results": "PASS",
            "Rich Results Type": "FAQ",
            "Severity": "Error",
            "Item Name": "FAQPage",
            "Rich Results Issue Type": "Missing field",
        }
    ]


def test_get_hreflang_multimap_tabs_emit_expected_rows() -> None:
    multimap_missing = _FakeCursor(
        ["MULTIMAP_KEY", "MULTIMAP_VALUE"],
        [("https://example.com/a", "https://example.com/b")],
    )
    missing_link = _FakeCursor(["HREF_LANG", "LINK_TYPE"], [("en-gb", 13)])
    missing_target = _FakeCursor(
        ["RESPONSE_CODE", "HTTP_RESPONSE_HEADER_COLLECTION"],
        [(404, None)],
    )
    multimap_inconsistent = _FakeCursor(
        ["MULTIMAP_KEY", "MULTIMAP_VALUE"],
        [("https://example.com/a", "https://example.com/b")],
    )
    inconsistent_link = _FakeCursor(["HREF_LANG", "LINK_TYPE"], [("en-gb", 13)])
    inconsistent_return = _FakeCursor(
        ["ENCODED_URL", "HREF_LANG"],
        [("https://example.com/c", "fr-fr")],
    )
    multimap_noncanonical = _FakeCursor(
        ["MULTIMAP_KEY", "MULTIMAP_VALUE"],
        [("https://example.com/a", "https://example.com/b")],
    )
    canonical_target = _FakeCursor(
        ["ENCODED_URL"],
        [("https://example.com/canonical-b",)],
    )
    multimap_noindex = _FakeCursor(
        ["MULTIMAP_KEY", "MULTIMAP_VALUE"],
        [("https://example.com/a", "https://example.com/b")],
    )
    noindex_link = _FakeCursor(["HREF_LANG", "LINK_TYPE"], [("en-gb", 13)])

    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _MultiCursorConnection(
        [
            multimap_missing,
            missing_link,
            missing_target,
            multimap_inconsistent,
            inconsistent_link,
            inconsistent_return,
            multimap_noncanonical,
            canonical_target,
            multimap_noindex,
            noindex_link,
        ]
    )
    backend._mapping = {
        "hreflang_missing_return_links.csv": [
            {"csv_column": "URL Missing Return Link", "db_expression": "NULL"},
            {"csv_column": "URL Not Returning Link", "db_expression": "NULL"},
            {"csv_column": "Expected Link", "db_expression": "NULL"},
            {"csv_column": "Response Code", "db_column": "RESPONSE_CODE"},
            {"csv_column": "hreflang", "db_expression": "NULL"},
        ],
        "hreflang_inconsistent_language_return_links.csv": [
            {
                "csv_column": "URL with Inconsistent Language Return Link",
                "db_expression": "NULL",
            },
            {"csv_column": "URL Target", "db_expression": "NULL"},
            {
                "csv_column": "URL Returning with Inconsistent Language",
                "db_expression": "NULL",
            },
            {"csv_column": "Expected Language", "db_expression": "NULL"},
            {"csv_column": "Actual Language", "db_expression": "NULL"},
        ],
        "hreflang_non_canonical_return_links.csv": [
            {"csv_column": "URL", "db_column": "ENCODED_URL"},
            {"csv_column": "Non Canonical Return Link URL", "db_expression": "NULL"},
            {"csv_column": "Canonical", "db_expression": "NULL"},
        ],
        "hreflang_no_index_return_links.csv": [
            {"csv_column": "URL", "db_column": "ENCODED_URL"},
            {"csv_column": "Noindex URL", "db_expression": "NULL"},
            {"csv_column": "Language", "db_expression": "NULL"},
        ],
    }

    assert list(backend.get_tab("hreflang_missing_return_links")) == [
        {
            "URL Missing Return Link": "https://example.com/a",
            "URL Not Returning Link": "https://example.com/b",
            "Expected Link": "https://example.com/a",
            "Response Code": 404,
            "hreflang": "en-gb",
        }
    ]
    assert list(backend.get_tab("hreflang_inconsistent_language_return_links")) == [
        {
            "URL with Inconsistent Language Return Link": "https://example.com/a",
            "URL Target": "https://example.com/b",
            "URL Returning with Inconsistent Language": "https://example.com/c",
            "Expected Language": "en-gb",
            "Actual Language": "fr-fr",
        }
    ]
    assert list(backend.get_tab("hreflang_non_canonical_return_links")) == [
        {
            "URL": "https://example.com/a",
            "Non Canonical Return Link URL": "https://example.com/b",
            "Canonical": "https://example.com/canonical-b",
        }
    ]
    assert list(backend.get_tab("hreflang_no_index_return_links")) == [
        {
            "URL": "https://example.com/a",
            "Noindex URL": "https://example.com/b",
            "Language": "en-gb",
        }
    ]


def test_get_tab_derives_ajax_pretty_and_ugly_urls() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "CRAWL_IN_ESCAPED_FRAGMENT_FORM"],
        [
            ("https://example.com/path?_escaped_fragment_=products%2Fone", True),
            ("https://example.com/path#!products/two", False),
            ("https://example.com/plain", False),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "javascript_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {
                "csv_column": "Pretty URL",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "ajax_url_variant",
                    "variant": "pretty",
                    "columns": ["CRAWL_IN_ESCAPED_FRAGMENT_FORM"],
                },
            },
            {
                "csv_column": "Ugly URL",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "ajax_url_variant",
                    "variant": "ugly",
                    "columns": ["CRAWL_IN_ESCAPED_FRAGMENT_FORM"],
                },
            },
        ]
    }

    rows = list(backend.get_tab("javascript_all"))

    assert rows == [
        {
            "Address": "https://example.com/path?_escaped_fragment_=products%2Fone",
            "Pretty URL": "https://example.com/path#!products%2Fone",
            "Ugly URL": "https://example.com/path?_escaped_fragment_=products%2Fone",
        },
        {
            "Address": "https://example.com/path#!products/two",
            "Pretty URL": "https://example.com/path#!products/two",
            "Ugly URL": "https://example.com/path?_escaped_fragment_=products%2Ftwo",
        },
        {
            "Address": "https://example.com/plain",
            "Pretty URL": "https://example.com/plain",
            "Ugly URL": "https://example.com/plain",
        },
    ]


def test_get_tab_derives_amphtml_link_from_original_content() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "ORIGINAL_CONTENT"],
        [
            (
                "https://example.com/article",
                _FakeClob('<html><head><link rel=\"amphtml\" href=\"/article/amp\" /></head></html>'),
            ),
            (
                "https://example.com/no-amp",
                _FakeClob("<html><head></head><body>No link</body></html>"),
            ),
        ],
        types=["VARCHAR", "CLOB"],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "internal_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {
                "csv_column": "amphtml Link Element",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "html_link_element",
                    "rel": "amphtml",
                    "columns": ["ORIGINAL_CONTENT"],
                },
            },
        ]
    }

    rows = list(backend.get_tab("internal_all"))

    assert rows == [
        {
            "Address": "https://example.com/article",
            "amphtml Link Element": "https://example.com/article/amp",
        },
        {
            "Address": "https://example.com/no-amp",
            "amphtml Link Element": None,
        },
    ]
    assert cursor.fetchall_called == 0
    assert cursor.fetchmany_calls == [1, 1, 1]


def test_get_tab_derives_mobile_alternate_link_from_original_content() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "ORIGINAL_CONTENT"],
        [
            (
                "https://example.com/article",
                _FakeClob(
                    '<html><head><link rel="alternate" media="only screen and (max-width: 640px)" href="/m/article" /></head></html>'
                ),
            ),
            (
                "https://example.com/no-mobile",
                _FakeClob("<html><head><link rel=\"alternate\" hreflang=\"en\" href=\"/en\" /></head></html>"),
            ),
        ],
        types=["VARCHAR", "CLOB"],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "internal_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {
                "csv_column": "Mobile Alternate Link",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "mobile_alternate_link",
                    "columns": ["ORIGINAL_CONTENT"],
                },
            },
        ]
    }

    rows = list(backend.get_tab("internal_all"))

    assert rows == [
        {
            "Address": "https://example.com/article",
            "Mobile Alternate Link": "https://example.com/m/article",
        },
        {
            "Address": "https://example.com/no-mobile",
            "Mobile Alternate Link": None,
        },
    ]
    assert cursor.fetchall_called == 0
    assert cursor.fetchmany_calls == [1, 1, 1]


def test_get_mobile_all_tab_derives_mobile_alternate_link() -> None:
    cursor = _FakeCursor(
        [
            "ENCODED_URL",
            "SF_REQUEST_ERROR_KEY",
            "VIEWPORT",
            "TARGET_SIZE",
            "CONTENT_WIDTH",
            "FONT_DISPLAY_SIZE",
            "ORIGINAL_CONTENT",
        ],
        [
            (
                "https://example.com/article",
                None,
                None,
                100,
                None,
                None,
                _FakeClob(
                    '<html><head><link rel="alternate" media="only screen and (max-width: 640px)" href="/m/article" /></head></html>'
                ),
            )
        ],
        types=["VARCHAR", "VARCHAR", "VARCHAR", "INTEGER", "INTEGER", "INTEGER", "CLOB"],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "mobile_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.PAGE_SPEED_API"},
            {
                "csv_column": "Mobile Alternate Link",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "mobile_alternate_link",
                    "columns": ["ORIGINAL_CONTENT"],
                },
            },
        ]
    }

    rows = list(backend.get_tab("mobile_all"))

    assert rows == [
        {
            "Address": "https://example.com/article",
            "Mobile Alternate Link": "https://example.com/m/article",
        }
    ]


def test_get_mobile_all_tab_derives_mobile_alternate_link() -> None:
    cursor = _FakeCursor(
        [
            "ENCODED_URL",
            "SF_REQUEST_ERROR_KEY",
            "VIEWPORT",
            "TARGET_SIZE",
            "CONTENT_WIDTH",
            "FONT_DISPLAY_SIZE",
            "ORIGINAL_CONTENT",
        ],
        [
            (
                "https://example.com/article",
                None,
                "width=device-width, initial-scale=1",
                0,
                0,
                0,
                _FakeClob(
                    '<html><head><link rel="alternate" media="only screen and (max-width: 640px)" href="/m/article" /></head></html>'
                ),
            ),
            (
                "https://example.com/no-mobile",
                "Timeout",
                None,
                3,
                1440,
                12,
                _FakeClob("<html><head></head><body>No mobile</body></html>"),
            ),
        ],
        types=["VARCHAR", "VARCHAR", "VARCHAR", "INTEGER", "INTEGER", "INTEGER", "CLOB"],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _FakeConnection(cursor)
    backend._mapping = {
        "mobile_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.PAGE_SPEED_API"},
            {"csv_column": "PSI Request Status", "db_expression": "NULL", "db_table": "APP.PAGE_SPEED_API"},
            {"csv_column": "Viewport", "db_column": "VIEWPORT", "db_table": "APP.PAGE_SPEED_API"},
            {"csv_column": "Target Size", "db_column": "TARGET_SIZE", "db_table": "APP.PAGE_SPEED_API"},
            {"csv_column": "Content Width", "db_column": "CONTENT_WIDTH", "db_table": "APP.PAGE_SPEED_API"},
            {"csv_column": "Font Display Size", "db_column": "FONT_DISPLAY_SIZE", "db_table": "APP.PAGE_SPEED_API"},
            {
                "csv_column": "Mobile Alternate Link",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
                "derived_extract": {
                    "type": "mobile_alternate_link",
                    "columns": ["ORIGINAL_CONTENT"],
                },
            },
        ]
    }

    rows = list(backend.get_tab("mobile_all"))

    assert rows == [
        {
            "Address": "https://example.com/article",
            "PSI Request Status": "Success",
            "Viewport": "width=device-width, initial-scale=1",
            "Target Size": 0,
            "Content Width": 0,
            "Font Display Size": 0,
            "Mobile Alternate Link": "https://example.com/m/article",
        },
        {
            "Address": "https://example.com/no-mobile",
            "PSI Request Status": "Timeout",
            "Viewport": None,
            "Target Size": 3,
            "Content Width": 1440,
            "Font Display Size": 12,
            "Mobile Alternate Link": None,
        },
    ]
    assert "LEFT JOIN APP.URLS u ON u.ENCODED_URL = p.ENCODED_URL" in (cursor.executed_sql or "")
    assert cursor.executed_params == []
    assert cursor.fetchall_called == 0
    assert cursor.fetchmany_calls == [1, 1, 1]
