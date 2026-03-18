from __future__ import annotations

import json
import sqlite3

from screamingfrog.backends.derby_backend import (
    DerbyBackend,
    _resolve_internal_alias_map,
    _resolve_internal_expression_selects,
    _resolve_internal_header_extract_map,
    _resolve_internal_mapping,
)


def _derby_like_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS APP")
    return conn


def _backend_for_mapping(conn: sqlite3.Connection, mapping: dict[str, list[dict[str, str]]]) -> DerbyBackend:
    backend = object.__new__(DerbyBackend)
    backend._conn = conn
    backend._mapping = mapping
    return backend


def _internal_backend_for_mapping(
    conn: sqlite3.Connection, mapping: dict[str, list[dict[str, str]]]
) -> DerbyBackend:
    backend = _backend_for_mapping(conn, mapping)
    backend._table, backend._column_map = _resolve_internal_mapping(mapping)
    backend._internal_columns = ["ENCODED_URL", "RESPONSE_CODE", "IS_INTERNAL", "HTTP_RESPONSE_HEADER_COLLECTION"]
    backend._internal_is_internal_col = "IS_INTERNAL"
    backend._internal_alias_map = _resolve_internal_alias_map(
        mapping, "APP.URLS", backend._internal_columns
    )
    backend._internal_header_extract_map = _resolve_internal_header_extract_map(
        mapping, "APP.URLS"
    )
    backend._internal_expr_selects = _resolve_internal_expression_selects(
        mapping, "APP.URLS"
    )
    return backend


def test_derby_internal_redirect_chain_filter_returns_only_internal_chain_urls() -> None:
    conn = _derby_like_connection()
    conn.execute(
        "CREATE TABLE APP.URLS ("
        "ENCODED_URL TEXT, RESPONSE_CODE INTEGER, IS_INTERNAL INTEGER, "
        "IS_REDIRECT INTEGER, REDIRECT_COUNT INTEGER)"
    )
    conn.executemany(
        "INSERT INTO APP.URLS "
        "(ENCODED_URL, RESPONSE_CODE, IS_INTERNAL, IS_REDIRECT, REDIRECT_COUNT) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("https://example.com/chain", 301, 1, 1, 2),
            ("https://example.com/single", 301, 1, 1, 0),
            ("https://example.com/external-chain", 301, 0, 1, 2),
            ("https://example.com/ok", 200, 1, 0, 0),
        ],
    )
    mapping = {
        "response_codes_internal_redirect_chain.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Status Code", "db_column": "RESPONSE_CODE", "db_table": "APP.URLS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("Response Codes", {"__gui__": "Internal Redirect Chain"}))

    assert [row["Address"] for row in rows] == ["https://example.com/chain"]
    assert [row["Status Code"] for row in rows] == [301]
    conn.close()


def test_derby_hreflang_not_using_canonical_filter_returns_expected_sources() -> None:
    conn = _derby_like_connection()
    conn.execute("CREATE TABLE APP.URLS (ENCODED_URL TEXT PRIMARY KEY, IS_CANONICALISED INTEGER)")
    conn.execute("CREATE TABLE APP.UNIQUE_URLS (ID INTEGER PRIMARY KEY, ENCODED_URL TEXT)")
    conn.execute(
        "CREATE TABLE APP.LINKS ("
        "SRC_ID INTEGER, DST_ID INTEGER, LINK_TYPE INTEGER, HREF_LANG TEXT)"
    )
    conn.executemany(
        "INSERT INTO APP.URLS (ENCODED_URL, IS_CANONICALISED) VALUES (?, ?)",
        [
            ("https://example.com/en/", 0),
            ("https://example.com/de/", 1),
            ("https://example.com/fr/", 0),
            ("https://example.com/fr-alt/", 0),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.UNIQUE_URLS (ID, ENCODED_URL) VALUES (?, ?)",
        [
            (1, "https://example.com/en/"),
            (2, "https://example.com/de/"),
            (3, "https://example.com/fr/"),
            (4, "https://example.com/fr-alt/"),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.LINKS (SRC_ID, DST_ID, LINK_TYPE, HREF_LANG) VALUES (?, ?, ?, ?)",
        [
            (1, 2, 13, "de"),
            (3, 4, 13, "fr"),
            (3, 2, 1, None),
        ],
    )
    mapping = {
        "hreflang_not_using_canonical.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("Hreflang", {"__gui__": "Not Using Canonical"}))

    assert [row["Address"] for row in rows] == ["https://example.com/en/"]
    conn.close()


def test_derby_internal_filters_support_expression_and_header_fields() -> None:
    conn = _derby_like_connection()
    conn.execute(
        "CREATE TABLE APP.URLS ("
        "ENCODED_URL TEXT, RESPONSE_CODE INTEGER, IS_INTERNAL INTEGER, "
        "HTTP_RESPONSE_HEADER_COLLECTION BLOB)"
    )
    header_blob = json.dumps(
        {
            "mHeaders": [
                {
                    "mName": "Location",
                    "mValue": ["https://example.com/final"],
                }
            ]
        }
    ).encode("utf-8")
    conn.executemany(
        "INSERT INTO APP.URLS "
        "(ENCODED_URL, RESPONSE_CODE, IS_INTERNAL, HTTP_RESPONSE_HEADER_COLLECTION) "
        "VALUES (?, ?, ?, ?)",
        [
            ("https://example.com/ok", 200, 1, header_blob),
            ("https://example.com/missing", 404, 1, None),
        ],
    )
    mapping = {
        "internal_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Status Code", "db_column": "RESPONSE_CODE", "db_table": "APP.URLS"},
            {
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN RESPONSE_CODE = 200 THEN 'Indexable' ELSE 'Non-Indexable' END",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Response Header: location",
                "db_column": "HTTP_RESPONSE_HEADER_COLLECTION",
                "db_table": "APP.URLS",
                "header_extract": {"type": "header_name", "name": "location"},
            },
        ]
    }
    backend = _internal_backend_for_mapping(conn, mapping)

    pages = list(backend.get_internal(filters={"indexability": "Indexable"}))
    header_pages = list(
        backend.get_internal(
            filters={"response_header_location": "https://example.com/final"}
        )
    )

    assert [page.address for page in pages] == ["https://example.com/ok"]
    assert backend.count("internal", filters={"indexability": "Indexable"}) == 1
    assert [page.address for page in header_pages] == ["https://example.com/ok"]
    assert (
        backend.count(
            "internal",
            filters={"response_header_location": "https://example.com/final"},
        )
        == 1
    )
    conn.close()


def test_derby_tab_filters_support_expression_and_header_fields() -> None:
    conn = _derby_like_connection()
    conn.execute(
        "CREATE TABLE APP.URLS ("
        "ENCODED_URL TEXT, RESPONSE_CODE INTEGER, HTTP_RESPONSE_HEADER_COLLECTION BLOB)"
    )
    header_blob = json.dumps(
        {
            "mHeaders": [
                {
                    "mName": "Link",
                    "mValue": ["<https://example.com/canonical>; rel=\"canonical\""],
                }
            ]
        }
    ).encode("utf-8")
    conn.executemany(
        "INSERT INTO APP.URLS "
        "(ENCODED_URL, RESPONSE_CODE, HTTP_RESPONSE_HEADER_COLLECTION) "
        "VALUES (?, ?, ?)",
        [
            ("https://example.com/ok", 200, header_blob),
            ("https://example.com/missing", 404, None),
        ],
    )
    mapping = {
        "canonicals_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN RESPONSE_CODE = 200 THEN 'Indexable' ELSE 'Non-Indexable' END",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "HTTP Canonical",
                "db_column": "HTTP_RESPONSE_HEADER_COLLECTION",
                "db_table": "APP.URLS",
                "header_extract": {"type": "link_rel", "rel": "canonical"},
            },
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    expr_rows = list(backend.get_tab("canonicals_all", {"indexability": "Indexable"}))
    header_rows = list(
        backend.get_tab(
            "canonicals_all",
            {"http_canonical": "https://example.com/canonical"},
        )
    )

    assert [row["Address"] for row in expr_rows] == ["https://example.com/ok"]
    assert [row["Address"] for row in header_rows] == ["https://example.com/ok"]
    conn.close()
