from __future__ import annotations

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
