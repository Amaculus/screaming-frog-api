from __future__ import annotations

import json
import gzip
import sqlite3

from screamingfrog.backends.derby_backend import (
    DerbyBackend,
    _normalize_gui_where_sql,
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


def test_derby_hreflang_issue_filename_applies_implicit_filter() -> None:
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
        ],
    )
    conn.executemany(
        "INSERT INTO APP.UNIQUE_URLS (ID, ENCODED_URL) VALUES (?, ?)",
        [
            (1, "https://example.com/en/"),
            (2, "https://example.com/de/"),
            (3, "https://example.com/fr/"),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.LINKS (SRC_ID, DST_ID, LINK_TYPE, HREF_LANG) VALUES (?, ?, ?, ?)",
        [
            (1, 2, 13, "de"),
            (3, 1, 13, "en"),
        ],
    )
    mapping = {
        "hreflang_not_using_canonical.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("hreflang_not_using_canonical.csv"))

    assert [row["Address"] for row in rows] == ["https://example.com/en/"]
    conn.close()


def test_derby_hreflang_missing_self_and_xdefault_require_hreflang() -> None:
    conn = _derby_like_connection()
    conn.execute("CREATE TABLE APP.URLS (ENCODED_URL TEXT PRIMARY KEY)")
    conn.execute("CREATE TABLE APP.UNIQUE_URLS (ID INTEGER PRIMARY KEY, ENCODED_URL TEXT)")
    conn.execute(
        "CREATE TABLE APP.LINKS ("
        "SRC_ID INTEGER, DST_ID INTEGER, LINK_TYPE INTEGER, HREF_LANG TEXT)"
    )
    conn.executemany(
        "INSERT INTO APP.URLS (ENCODED_URL) VALUES (?)",
        [
            ("https://example.com/en/",),
            ("https://example.com/de/",),
            ("https://example.com/orphan/",),
            ("https://example.com/default/",),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.UNIQUE_URLS (ID, ENCODED_URL) VALUES (?, ?)",
        [
            (1, "https://example.com/en/"),
            (2, "https://example.com/de/"),
            (3, "https://example.com/orphan/"),
            (4, "https://example.com/default/"),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.LINKS (SRC_ID, DST_ID, LINK_TYPE, HREF_LANG) VALUES (?, ?, ?, ?)",
        [
            (1, 2, 13, "de"),
            (2, 2, 13, "de"),
            (4, 1, 13, "x-default"),
        ],
    )
    mapping = {
        "hreflang_missing_self_reference.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ],
        "hreflang_missing_xdefault.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ],
    }
    backend = _backend_for_mapping(conn, mapping)

    missing_self = list(backend.get_tab("hreflang_missing_self_reference.csv"))
    missing_xdefault = list(backend.get_tab("hreflang_missing_xdefault.csv"))

    assert [row["Address"] for row in missing_self] == [
        "https://example.com/en/",
        "https://example.com/default/",
    ]
    assert [row["Address"] for row in missing_xdefault] == [
        "https://example.com/en/",
        "https://example.com/de/",
    ]
    conn.close()


def test_derby_hreflang_noindex_filename_applies_implicit_multimap_filter() -> None:
    conn = _derby_like_connection()
    conn.execute("CREATE TABLE APP.URLS (ENCODED_URL TEXT PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE APP.MULTIMAP_HREF_LANG_NO_INDEX_CONFIRMATION ("
        "MULTIMAP_KEY TEXT, MULTIMAP_VALUE TEXT)"
    )
    conn.executemany(
        "INSERT INTO APP.URLS (ENCODED_URL) VALUES (?)",
        [
            ("https://example.com/a/",),
            ("https://example.com/b/",),
        ],
    )
    conn.execute(
        "INSERT INTO APP.MULTIMAP_HREF_LANG_NO_INDEX_CONFIRMATION "
        "(MULTIMAP_KEY, MULTIMAP_VALUE) VALUES (?, ?)",
        ("https://example.com/a/", "https://example.com/noindex/"),
    )
    mapping = {
        "hreflang_noindex_return_links.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("hreflang_noindex_return_links.csv"))

    assert [row["Address"] for row in rows] == ["https://example.com/a/"]
    conn.close()


def test_derby_hreflang_unlinked_filter_returns_only_hreflang_only_destinations() -> None:
    conn = _derby_like_connection()
    conn.execute("CREATE TABLE APP.UNIQUE_URLS (ID INTEGER PRIMARY KEY, ENCODED_URL TEXT)")
    conn.execute(
        "CREATE TABLE APP.LINKS ("
        "SRC_ID INTEGER, DST_ID INTEGER, LINK_TYPE INTEGER, HREF_LANG TEXT, "
        "PATH_TYPE TEXT, ORIGIN INTEGER)"
    )
    conn.execute(
        "CREATE TABLE APP.INLINK_COUNTS (ENCODED_URL TEXT PRIMARY KEY, NUM_HYPER_LINKS INTEGER)"
    )
    conn.execute(
        "CREATE TABLE APP.URLS (ENCODED_URL TEXT PRIMARY KEY, RESPONSE_CODE INTEGER, RESPONSE_MSG TEXT)"
    )
    conn.executemany(
        "INSERT INTO APP.UNIQUE_URLS (ID, ENCODED_URL) VALUES (?, ?)",
        [
            (1, "https://example.com/source-en/"),
            (2, "https://example.com/only-hreflang/"),
            (3, "https://example.com/normal-target/"),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.URLS (ENCODED_URL, RESPONSE_CODE, RESPONSE_MSG) VALUES (?, ?, ?)",
        [
            ("https://example.com/only-hreflang/", 200, "OK"),
            ("https://example.com/normal-target/", 200, "OK"),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.INLINK_COUNTS (ENCODED_URL, NUM_HYPER_LINKS) VALUES (?, ?)",
        [
            ("https://example.com/only-hreflang/", 0),
            ("https://example.com/normal-target/", 3),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.LINKS (SRC_ID, DST_ID, LINK_TYPE, HREF_LANG, PATH_TYPE, ORIGIN) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (1, 2, 13, "en-gb", "html", 1),
            (1, 3, 13, "de-de", "html", 1),
        ],
    )
    mapping = {
        "hreflang_unlinked_hreflang_urls.csv": [
            {"csv_column": "Type", "db_column": "LINK_TYPE", "db_table": "APP.LINKS"},
            {"csv_column": "hreflang", "db_column": "HREF_LANG", "db_table": "APP.LINKS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("Hreflang", {"__gui__": "Unlinked hreflang URLs"}))

    assert rows == [{"Type": 13, "hreflang": "en-gb"}]
    conn.close()


def test_derby_structured_data_issue_tabs_do_not_return_every_url() -> None:
    conn = _derby_like_connection()
    conn.execute(
        "CREATE TABLE APP.URLS ("
        "ENCODED_URL TEXT PRIMARY KEY, SERIALISED_STRUCTURED_DATA BLOB, PARSE_ERROR_MSG TEXT)"
    )
    conn.execute(
        "CREATE TABLE APP.URL_INSPECTION ("
        "ENCODED_URL TEXT PRIMARY KEY, RICH_RESULTS_TYPES TEXT, "
        "RICH_RESULTS_TYPE_ERRORS INTEGER, RICH_RESULTS_TYPE_WARNINGS INTEGER)"
    )
    structured_blob = b"JSONLD" + gzip.compress(b'{"@type": "Article"}')
    conn.executemany(
        "INSERT INTO APP.URLS "
        "(ENCODED_URL, SERIALISED_STRUCTURED_DATA, PARSE_ERROR_MSG) VALUES (?, ?, ?)",
        [
            ("https://example.com/missing/", None, None),
            ("https://example.com/valid/", structured_blob, None),
            ("https://example.com/error/", structured_blob, "invalid json-ld"),
        ],
    )
    conn.executemany(
        "INSERT INTO APP.URL_INSPECTION "
        "(ENCODED_URL, RICH_RESULTS_TYPES, RICH_RESULTS_TYPE_ERRORS, RICH_RESULTS_TYPE_WARNINGS) "
        "VALUES (?, ?, ?, ?)",
        [
            ("https://example.com/missing/", None, 0, 0),
            ("https://example.com/valid/", "Article", 0, 0),
            ("https://example.com/error/", "Article", 2, 1),
        ],
    )
    mapping = {
        "structured_data_missing.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ],
        "structured_data_validation_errors.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Errors", "db_column": "RICH_RESULTS_TYPE_ERRORS", "db_table": "APP.URL_INSPECTION"},
        ],
        "structured_data_validation_warnings.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Warnings", "db_column": "RICH_RESULTS_TYPE_WARNINGS", "db_table": "APP.URL_INSPECTION"},
        ],
        "structured_data_parse_errors.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Parse Error", "db_column": "PARSE_ERROR_MSG", "db_table": "APP.URLS"},
        ],
    }
    backend = _backend_for_mapping(conn, mapping)

    assert [row["Address"] for row in backend.get_tab("structured_data_missing.csv")] == [
        "https://example.com/missing/"
    ]
    assert [row["Address"] for row in backend.get_tab("structured_data_validation_errors.csv")] == [
        "https://example.com/error/"
    ]
    assert [row["Address"] for row in backend.get_tab("structured_data_validation_warnings.csv")] == [
        "https://example.com/error/"
    ]
    assert [row["Address"] for row in backend.get_tab("structured_data_parse_errors.csv")] == [
        "https://example.com/error/"
    ]
    conn.close()


def test_normalize_gui_where_sql_converts_boolean_integer_comparisons() -> None:
    sql = (
        "IS_INTERNAL = 1 AND BLOCKED_BY_ROBOTS_TXT = 1 "
        "AND RESPONSE_CODE = 0 AND j.CANONICAL_OUTSIDE_HEAD = 1"
    )

    assert _normalize_gui_where_sql(sql) == (
        "IS_INTERNAL = TRUE AND BLOCKED_BY_ROBOTS_TXT = TRUE "
        "AND RESPONSE_CODE = 0 AND j.CANONICAL_OUTSIDE_HEAD = TRUE"
    )


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


def test_derby_gui_filter_join_qualifies_ambiguous_select_columns() -> None:
    """A GUI filter joining a table that also has ENCODED_URL must not be ambiguous.

    Regression: the SELECT list was built with bare column names before the
    join was appended, so `SELECT ENCODED_URL ... FROM APP.URLS INNER JOIN
    APP.DUPLICATES_TITLE j ON ...` made Derby raise
    "Column name 'ENCODED_URL' is in more than one table in the FROM list."
    sqlite raises the same class of error ("ambiguous column name").
    """
    conn = _derby_like_connection()
    conn.execute("CREATE TABLE APP.URLS (ENCODED_URL TEXT PRIMARY KEY, TITLE_1 TEXT)")
    conn.execute("CREATE TABLE APP.DUPLICATES_TITLE (ENCODED_URL TEXT, DUPLICATE_KEY TEXT)")
    conn.execute(
        "INSERT INTO APP.URLS VALUES ('https://example.com/a/', 'Same title'), "
        "('https://example.com/b/', 'Same title'), ('https://example.com/c/', 'Unique')"
    )
    conn.execute(
        "INSERT INTO APP.DUPLICATES_TITLE VALUES ('https://example.com/a/', 'k1'), "
        "('https://example.com/b/', 'k1')"
    )
    mapping = {
        "page_titles_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
            {"csv_column": "Title 1", "db_column": "TITLE_1", "db_table": "APP.URLS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("Page Titles", filters={"__gui__": "Duplicate"}))

    assert [row["Address"] for row in rows] == [
        "https://example.com/a/",
        "https://example.com/b/",
    ]
    conn.close()


def test_derby_gui_filter_join_count_matches_rows() -> None:
    """tab_count() and get_tab() must agree for a joined GUI filter.

    tab_count emits SELECT COUNT(*), which was never ambiguous, so .count()
    succeeded while .collect() raised on the very same filter.
    """
    conn = _derby_like_connection()
    conn.execute("CREATE TABLE APP.URLS (ENCODED_URL TEXT PRIMARY KEY, TITLE_1 TEXT)")
    conn.execute(
        "CREATE TABLE APP.HTML_VALIDATION_DATA (ENCODED_URL TEXT, TITLE_OUTSIDE_HEAD BOOLEAN)"
    )
    conn.execute(
        "INSERT INTO APP.URLS VALUES ('https://example.com/a/', 'A'), "
        "('https://example.com/b/', 'B')"
    )
    conn.execute(
        "INSERT INTO APP.HTML_VALIDATION_DATA VALUES ('https://example.com/a/', 1), "
        "('https://example.com/b/', 0)"
    )
    mapping = {
        "page_titles_all.csv": [
            {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        ]
    }
    backend = _backend_for_mapping(conn, mapping)

    rows = list(backend.get_tab("Page Titles", filters={"__gui__": "Outside <head>"}))
    count = backend.tab_count("Page Titles", filters={"__gui__": "Outside <head>"})

    assert [row["Address"] for row in rows] == ["https://example.com/a/"]
    assert count == len(rows)
    conn.close()
