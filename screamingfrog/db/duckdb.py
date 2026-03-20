from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from screamingfrog.db.packaging import find_project_dir


DEFAULT_DUCKDB_TABLES: tuple[str, ...] = ("APP.URLS", "APP.LINKS", "APP.UNIQUE_URLS")
DEFAULT_DUCKDB_TABS: tuple[str, ...] = (
    "internal_all",
    "all_inlinks",
    "all_outlinks",
    "redirect_chains",
    "canonical_chains",
    "redirect_and_canonical_chains",
)
_FETCH_BATCH_SIZE = 1000


def export_duckdb_from_derby(
    db_path: str,
    duckdb_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    tabs: Sequence[str] | str | None = None,
    if_exists: str = "replace",
    source_label: str | None = None,
    mapping_path: str | None = None,
    derby_jar: str | None = None,
) -> Path:
    from screamingfrog.backends.derby_backend import DerbyBackend

    backend = DerbyBackend(db_path, mapping_path=mapping_path, derby_jar=derby_jar)
    label = source_label or str(Path(db_path).resolve())
    return export_duckdb_from_backend(
        backend,
        duckdb_path,
        tables=tables,
        tabs=tabs,
        if_exists=if_exists,
        source_label=label,
    )


def export_duckdb_from_db_id(
    db_id: str,
    duckdb_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    tabs: Sequence[str] | str | None = None,
    if_exists: str = "replace",
    project_root: str | Path | None = None,
    mapping_path: str | None = None,
    derby_jar: str | None = None,
) -> Path:
    project_dir = find_project_dir(db_id, project_root=project_root)
    return export_duckdb_from_derby(
        str(project_dir),
        duckdb_path,
        tables=tables,
        tabs=tabs,
        if_exists=if_exists,
        source_label=db_id,
        mapping_path=mapping_path,
        derby_jar=derby_jar,
    )


def export_duckdb_from_backend(
    backend: Any,
    duckdb_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    tabs: Sequence[str] | str | None = None,
    if_exists: str = "replace",
    source_label: str | None = None,
) -> Path:
    mode = str(if_exists).strip().lower()
    if mode not in {"replace", "skip"}:
        raise ValueError("if_exists must be 'replace' or 'skip'")

    relation_tables = tuple(tables or DEFAULT_DUCKDB_TABLES)
    materialized_tabs = _resolve_export_tabs(backend, tabs)
    target = Path(duckdb_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    duckdb = _import_duckdb()
    conn = duckdb.connect(str(target))
    try:
        _ensure_metadata_tables(conn)
        existing = _get_import_metadata(conn)
        label = source_label or getattr(getattr(backend, "db_path", None), "name", None) or "crawl"

        if existing and mode == "skip" and existing.get("source_label") == label:
            return target

        if existing and mode == "replace":
            _drop_exported_objects(conn)

        conn.execute("CREATE SCHEMA IF NOT EXISTS app")

        exported_objects: list[tuple[str, str, str]] = []
        for raw_name in relation_tables:
            relation_name = _raw_relation_name(raw_name)
            rows = backend.raw(raw_name)
            if _write_relation(conn, relation_name, rows):
                exported_objects.append((raw_name.upper(), "raw", relation_name))

        for tab_name in materialized_tabs:
            normalized = _normalize_tab_name(tab_name)
            relation_name = _tab_relation_name(normalized)
            rows = backend.get_tab(normalized)
            if _write_relation(conn, relation_name, rows):
                exported_objects.append((normalized, "tab", relation_name))

        _store_export_metadata(
            conn,
            source_label=str(label),
            objects=exported_objects,
        )
        return target
    finally:
        conn.close()


def iter_relation_rows(conn: Any, relation_name: str) -> Iterator[dict[str, Any]]:
    cursor = conn.execute(f"SELECT * FROM {relation_name}")
    columns = [desc[0] for desc in cursor.description or []]
    for row in iter_cursor_rows(cursor):
        yield {col: val for col, val in zip(columns, row)}


def list_exported_tabs(conn: Any) -> list[str]:
    cursor = conn.execute(
        "SELECT export_name FROM sf_alpha_exports WHERE kind = 'tab' ORDER BY export_name"
    )
    return [str(row[0]) for row in cursor.fetchall()]


def resolve_relation_name(conn: Any, kind: str, export_name: str) -> str | None:
    normalized = export_name if kind == "raw" else _normalize_tab_name(export_name)
    cursor = conn.execute(
        "SELECT relation_name FROM sf_alpha_exports WHERE kind = ? AND export_name = ? LIMIT 1",
        [kind, normalized.upper() if kind == "raw" else normalized],
    )
    row = cursor.fetchone()
    if row:
        return str(row[0])
    if kind == "raw":
        candidate = _raw_relation_name(export_name)
        return candidate if _relation_exists(conn, candidate) else None
    candidate = _tab_relation_name(normalized)
    return candidate if _relation_exists(conn, candidate) else None


def _write_relation(conn: Any, relation_name: str, rows: Iterable[Mapping[str, Any]]) -> bool:
    iterator = iter(rows)
    buffered: list[Mapping[str, Any]] = []
    for _ in range(200):
        try:
            buffered.append(next(iterator))
        except StopIteration:
            break
    if not buffered:
        return False

    columns = _ordered_columns(buffered)
    type_map = _infer_duckdb_types(buffered, columns)
    _drop_relation(conn, relation_name)
    _create_relation(conn, relation_name, columns, type_map)
    _insert_rows(conn, relation_name, columns, buffered)
    _insert_rows(conn, relation_name, columns, iterator)
    return True


def _ordered_columns(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    seen: set[str] = set()
    columns: list[str] = []
    for row in rows:
        for key in row.keys():
            text = str(key)
            if text in seen:
                continue
            seen.add(text)
            columns.append(text)
    return columns


def _infer_duckdb_types(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> dict[str, str]:
    inferred: dict[str, str] = {}
    precedence = {"BOOLEAN": 0, "BIGINT": 1, "DOUBLE": 2, "TIMESTAMP": 3, "BLOB": 4, "VARCHAR": 5}
    for col in columns:
        best: str | None = None
        for row in rows:
            value = row.get(col)
            current = _duckdb_type_for_value(value)
            if best is None or precedence[current] > precedence[best]:
                best = current
        inferred[col] = best or "VARCHAR"
    return inferred


def _duckdb_type_for_value(value: Any) -> str:
    if value is None:
        return "VARCHAR"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "BLOB"
    return "VARCHAR"


def _create_relation(conn: Any, relation_name: str, columns: Sequence[str], type_map: Mapping[str, str]) -> None:
    column_sql = ", ".join(
        f'{_quote_identifier(column)} {type_map.get(column, "VARCHAR")}' for column in columns
    )
    conn.execute(f"CREATE TABLE {relation_name} ({column_sql})")


def _insert_rows(
    conn: Any,
    relation_name: str,
    columns: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    placeholders = ", ".join("?" for _ in columns)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    sql = f"INSERT INTO {relation_name} ({quoted_columns}) VALUES ({placeholders})"
    batch: list[tuple[Any, ...]] = []
    for row in rows:
        batch.append(tuple(_convert_duckdb_value(row.get(column)) for column in columns))
        if len(batch) >= 1000:
            conn.executemany(sql, batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)


def _convert_duckdb_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Path):
        return str(value)
    return value


def _ensure_metadata_tables(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sf_alpha_imports (
            source_label VARCHAR,
            imported_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sf_alpha_exports (
            export_name VARCHAR,
            kind VARCHAR,
            relation_name VARCHAR
        )
        """
    )


def _get_import_metadata(conn: Any) -> dict[str, Any] | None:
    cursor = conn.execute(
        "SELECT source_label, imported_at FROM sf_alpha_imports LIMIT 1"
    )
    row = cursor.fetchone()
    if not row:
        return None
    return {"source_label": row[0], "imported_at": row[1]}


def _drop_exported_objects(conn: Any) -> None:
    cursor = conn.execute("SELECT relation_name FROM sf_alpha_exports")
    relations = [str(row[0]) for row in cursor.fetchall()]
    for relation in relations:
        _drop_relation(conn, relation)
    conn.execute("DELETE FROM sf_alpha_exports")
    conn.execute("DELETE FROM sf_alpha_imports")


def _store_export_metadata(
    conn: Any,
    *,
    source_label: str,
    objects: Sequence[tuple[str, str, str]],
) -> None:
    conn.execute("DELETE FROM sf_alpha_exports")
    conn.execute("DELETE FROM sf_alpha_imports")
    conn.execute(
        "INSERT INTO sf_alpha_imports (source_label, imported_at) VALUES (?, ?)",
        [source_label, datetime.now(timezone.utc)],
    )
    conn.executemany(
        "INSERT INTO sf_alpha_exports (export_name, kind, relation_name) VALUES (?, ?, ?)",
        list(objects),
    )


def _drop_relation(conn: Any, relation_name: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {relation_name}")


def _relation_exists(conn: Any, relation_name: str) -> bool:
    schema_name, _, table_name = relation_name.partition(".")
    if not table_name:
        schema_name, table_name = "main", schema_name
    cursor = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = lower(?)
          AND lower(table_name) = lower(?)
        LIMIT 1
        """,
        [schema_name, table_name],
    )
    return cursor.fetchone() is not None


def _raw_relation_name(raw_name: str) -> str:
    upper = str(raw_name).strip().upper()
    if "." in upper:
        schema_name, table_name = upper.split(".", 1)
    else:
        schema_name, table_name = "APP", upper
    return f"{schema_name.lower()}.{table_name.lower()}"


def _normalize_tab_name(tab_name: str) -> str:
    name = str(tab_name).strip()
    if not name.lower().endswith(".csv"):
        name = f"{name}.csv"
    return name.lower()


def _tab_relation_name(tab_name: str) -> str:
    stem = Path(tab_name).stem
    safe = "".join(ch if ch.isalnum() else "_" for ch in stem)
    return f"main.sf_tab_{safe}"


def iter_cursor_rows(cursor: Any, batch_size: int = _FETCH_BATCH_SIZE) -> Iterator[tuple[Any, ...]]:
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


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _resolve_export_tabs(backend: Any, tabs: Sequence[str] | str | None) -> tuple[str, ...]:
    if tabs is None:
        return DEFAULT_DUCKDB_TABS
    if isinstance(tabs, str):
        if tabs.strip().lower() != "all":
            return (_normalize_tab_name(tabs),)
        return tuple(_normalize_tab_name(name) for name in backend.list_tabs())
    return tuple(_normalize_tab_name(name) for name in tabs)


def _import_duckdb():
    try:
        import duckdb
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency error path
        raise ImportError("duckdb is required for DuckDB export support.") from exc
    return duckdb
