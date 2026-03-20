from __future__ import annotations

from typing import Any

from screamingfrog.backends.db_backend import DatabaseBackend, _build_sqlite_where
from screamingfrog.backends.duckdb_backend import DuckDBBackend
from screamingfrog.db.duckdb import iter_relation_rows


class _FakeCursor:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.description = [(col,) for col in columns]
        self._rows = list(rows)
        self._index = 0
        self.executed_sql: str | None = None
        self.executed_params: list[Any] | None = None
        self.fetchall_called = 0
        self.fetchone_called = 0
        self.fetchmany_calls: list[int] = []

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
        return list(self._rows[self._index :])

    def fetchone(self) -> tuple[Any, ...] | None:
        self.fetchone_called += 1
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row


class _FakeConnection:
    def __init__(self, cursors: list[_FakeCursor]) -> None:
        self._cursors = list(cursors)
        self._index = 0

    def execute(self, sql: str, params: list[Any] | None = None) -> _FakeCursor:
        if self._index >= len(self._cursors):
            raise AssertionError("No fake cursor left for execute()")
        cursor = self._cursors[self._index]
        self._index += 1
        cursor.executed_sql = sql
        cursor.executed_params = list(params or [])
        return cursor


def test_sqlite_build_where_handles_empty_sequences() -> None:
    where, params = _build_sqlite_where({"Status Code": []}, {"status_code": "status_code"})

    assert where == "1=0"
    assert params == []


def test_sqlite_backend_streams_get_tab_rows() -> None:
    cursor = _FakeCursor(
        ["Address", "Status Code"],
        [("https://example.com/", 200), ("https://example.com/missing", 404)],
    )
    backend = DatabaseBackend.__new__(DatabaseBackend)
    backend.conn = _FakeConnection([cursor])
    backend._internal_columns = ["address", "status_code"]
    backend._internal_column_map = {"address": "address", "status_code": "status_code"}

    rows = list(backend.get_tab("internal_all"))

    assert rows == [
        {"Address": "https://example.com/", "Status Code": 200},
        {"Address": "https://example.com/missing", "Status Code": 404},
    ]
    assert cursor.fetchall_called == 0
    assert len(cursor.fetchmany_calls) >= 1


def test_duckdb_backend_streams_relation_rows() -> None:
    cursor = _FakeCursor(
        ["Address", "Status Code"],
        [("https://example.com/", 200), ("https://example.com/missing", 404)],
    )
    backend = DuckDBBackend.__new__(DuckDBBackend)
    backend.conn = _FakeConnection([cursor])
    backend._get_relation_columns = lambda relation_name: ["Address", "Status Code"]  # type: ignore[method-assign]

    rows = list(backend._iter_relation("main.sf_tab_internal_all"))

    assert rows == [
        {"Address": "https://example.com/", "Status Code": 200},
        {"Address": "https://example.com/missing", "Status Code": 404},
    ]
    assert cursor.fetchall_called == 0
    assert len(cursor.fetchmany_calls) >= 1


def test_duckdb_backend_count_uses_sql_pushdown_when_possible() -> None:
    count_cursor = _FakeCursor(["count_star()"], [(3,)])
    backend = DuckDBBackend.__new__(DuckDBBackend)
    backend.conn = _FakeConnection([count_cursor])
    backend._internal_relation = "main.sf_tab_internal_all"
    backend._internal_columns = ["Address", "Status Code"]

    count = backend.count("internal", filters={"Status Code": 404})

    assert count == 3
    assert count_cursor.executed_sql == (
        'SELECT COUNT(*) FROM (SELECT * FROM main.sf_tab_internal_all WHERE "Status Code" = ?) AS sf_count'
    )
    assert count_cursor.executed_params == [404]
    assert count_cursor.fetchone_called == 1
    assert count_cursor.fetchall_called == 0


def test_duckdb_iter_relation_rows_streams_without_fetchall() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "RESPONSE_CODE"],
        [("https://example.com/", 200), ("https://example.com/missing", 404)],
    )
    conn = _FakeConnection([cursor])

    rows = list(iter_relation_rows(conn, "app.urls"))

    assert rows == [
        {"ENCODED_URL": "https://example.com/", "RESPONSE_CODE": 200},
        {"ENCODED_URL": "https://example.com/missing", "RESPONSE_CODE": 404},
    ]
    assert cursor.executed_sql == "SELECT * FROM app.urls"
    assert cursor.fetchall_called == 0
    assert len(cursor.fetchmany_calls) >= 1
