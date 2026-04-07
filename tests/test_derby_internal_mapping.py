from __future__ import annotations

from screamingfrog.backends.derby_backend import (
    _build_supplementary_map,
    _build_where_from_entries,
    _compile_internal_filters,
    _expression_references_absent_column,
    _expression_references_absent_table,
    _extract_header_value,
    _fetch_existing_tables,
    _header_extract_column,
    _normalize_select_expression,
    _resolve_tab_entries,
    _resolve_internal_alias_map,
    _resolve_internal_expression_selects,
    _resolve_internal_header_extract_map,
)


class _MetadataCursor:
    def __init__(self, rows: list[tuple[str, ...]]) -> None:
        self._rows = rows
        self.executed_sql: str | None = None
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed_sql = sql

    def fetchall(self) -> list[tuple[str, ...]]:
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)

    def close(self) -> None:
        self.closed = True


class _NonIterableMetadataCursor(_MetadataCursor):
    __iter__ = None


class _MetadataConnection:
    def __init__(self, cursor: _MetadataCursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _MetadataCursor:
        return self._cursor


def test_resolve_internal_alias_map_uses_first_direct_mapping_per_csv_column() -> None:
    mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Address",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Status Code",
                "db_column": "RESPONSE_CODE",
                "db_table": "APP.URLS",
            },
            {
                # Expression columns are intentionally ignored for aliasing.
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN 1=1 THEN 'Indexable' END",
                "db_table": "APP.URLS",
            },
            {
                # Duplicate CSV mapping should not override the first one.
                "csv_column": "Status Code",
                "db_column": "STATUS_CODE_ALT",
                "db_table": "APP.URLS",
            },
            {
                # Different table should be ignored.
                "csv_column": "Status Code",
                "db_column": "STATUS_CODE_OTHER",
                "db_table": "APP.OTHER",
            },
            {
                # Header-derived entries should not be direct aliases.
                "csv_column": 'HTTP rel="next" 1',
                "db_column": "HTTP_RESPONSE_HEADER_COLLECTION",
                "db_table": "APP.URLS",
                "header_extract": {"type": "link_rel", "rel": "next"},
            },
        ]
    }

    aliases = _resolve_internal_alias_map(
        mapping, "APP.URLS", ["ENCODED_URL", "RESPONSE_CODE"]
    )

    assert aliases == {
        "Address": "ENCODED_URL",
        "Status Code": "RESPONSE_CODE",
    }


def test_resolve_internal_header_extract_map_returns_header_entries() -> None:
    mapping = {
        "internal_all.csv": [
            {
                "csv_column": 'HTTP rel="next" 1',
                "db_column": "HTTP_RESPONSE_HEADER_COLLECTION",
                "db_table": "APP.URLS",
                "header_extract": {"type": "link_rel", "rel": "next"},
            },
            {
                # Duplicate key should keep first mapping.
                "csv_column": 'HTTP rel="next" 1',
                "db_column": "HTTP_RESPONSE_HEADER_COLLECTION",
                "db_table": "APP.URLS",
                "header_extract": {"type": "link_rel", "rel": "prev"},
            },
            {
                "csv_column": "Status Code",
                "db_column": "RESPONSE_CODE",
                "db_table": "APP.URLS",
            },
        ]
    }

    extracts = _resolve_internal_header_extract_map(mapping, "APP.URLS")

    assert extracts == {'HTTP rel="next" 1': {"type": "link_rel", "rel": "next"}}


def test_extract_header_value_returns_joined_named_header_values() -> None:
    result = _extract_header_value(
        {"type": "header_name", "name": "cache-control"},
        {"cache-control": ["public", "max-age=600"]},
        [],
    )

    assert result == "public, max-age=600"


def test_header_extract_column_defaults_to_response_headers() -> None:
    assert _header_extract_column({"type": "header_name", "name": "server"}) == (
        "HTTP_RESPONSE_HEADER_COLLECTION"
    )
    assert _header_extract_column(
        {
            "type": "header_name",
            "name": "user-agent",
            "column": "HTTP_REQUEST_HEADER_COLLECTION",
        }
    ) == "HTTP_REQUEST_HEADER_COLLECTION"


def test_resolve_internal_expression_selects_returns_deduped_expr_aliases() -> None:
    mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN 1=1 THEN 'Indexable' ELSE 'Non-Indexable' END",
                "db_table": "APP.URLS",
            },
            {
                # Duplicate CSV mapping should not override the first one.
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN 1=1 THEN 'X' END",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Indexability Status",
                "db_expression": "CASE WHEN 1=1 THEN 'ok' END",
                "db_table": "APP.URLS",
            },
            {
                # Different table should be ignored.
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN 1=1 THEN 'other' END",
                "db_table": "APP.OTHER",
            },
        ]
    }

    selects = _resolve_internal_expression_selects(mapping, "APP.URLS")

    assert selects == [
        (
            "SF_EXPR_0",
            "Indexability",
            "CASE WHEN 1=1 THEN 'Indexable' ELSE 'Non-Indexable' END",
        ),
        ("SF_EXPR_1", "Indexability Status", "CASE WHEN 1=1 THEN 'ok' END"),
    ]


def test_normalize_select_expression_casts_bare_null_literal() -> None:
    assert _normalize_select_expression("NULL") == "CAST(NULL AS VARCHAR(1))"
    assert _normalize_select_expression(" null ") == "CAST(NULL AS VARCHAR(1))"
    assert _normalize_select_expression("CAST(NULL AS INTEGER)") == "CAST(NULL AS INTEGER)"
    assert _normalize_select_expression("CAST(NULL AS VARCHAR(1))") == "CAST(NULL AS VARCHAR(1))"
    assert _normalize_select_expression("CAST(IMAGE_WIDTH AS VARCHAR(20))") == "TRIM(CHAR(IMAGE_WIDTH))"
    assert _normalize_select_expression(
        "CASE WHEN IMAGE_WIDTH IS NOT NULL THEN CAST(IMAGE_WIDTH AS VARCHAR(20)) ELSE NULL END"
    ) == (
        "CASE WHEN IMAGE_WIDTH IS NOT NULL THEN TRIM(CHAR(IMAGE_WIDTH)) "
        "ELSE CAST(NULL AS VARCHAR(1)) END"
    )
    assert _normalize_select_expression("CASE WHEN IMAGE_WIDTH IS NOT NULL THEN IMAGE_WIDTH ELSE NULL END") == (
        "CASE WHEN IMAGE_WIDTH IS NOT NULL THEN IMAGE_WIDTH ELSE NULL END"
    )


def test_resolve_tab_entries_tries_gui_key_without_hyphen() -> None:
    mapping = {
        "structured_data_jsonld_urls.csv": [
            {
                "csv_column": "Address JSONLD",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
            },
        ],
        "structured_data_all.csv": [
            {
                "csv_column": "Address All",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
            },
        ],
    }

    table, entries, gui_defs, supplementary = _resolve_tab_entries(
        mapping, "Structured Data", "JSON-LD URLs"
    )

    assert table == "APP.URLS"
    assert [entry["csv_column"] for entry in entries] == ["Address JSONLD"]
    assert gui_defs
    assert supplementary == []


def test_resolve_tab_entries_includes_supplementary_encoded_url_columns() -> None:
    mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Address",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Status Code",
                "db_column": "RESPONSE_CODE",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Performance Score",
                "db_column": "PERFORMANCE_SCORE",
                "db_table": "APP.PAGE_SPEED_API",
            },
            {
                "csv_column": "Spelling Errors",
                "db_column": "SPELLING_ERRORS",
                "db_table": "APP.LANGUAGE_ERROR",
            },
        ]
    }

    table, entries, gui_defs, supplementary = _resolve_tab_entries(
        mapping, "internal_all", None
    )

    assert table == "APP.URLS"
    assert gui_defs == []
    assert [entry["csv_column"] for entry in entries] == ["Address", "Status Code"]
    assert [entry["csv_column"] for entry in supplementary] == [
        "Performance Score",
        "Spelling Errors",
    ]


def test_resolve_tab_entries_skips_non_lookup_tables_for_supplementary() -> None:
    mapping = {
        "spelling_and_grammar_errors.csv": [
            {
                "csv_column": "Spelling Errors",
                "db_column": "SPELLING_ERRORS",
                "db_table": "APP.LANGUAGE_ERROR",
            },
            {
                "csv_column": "Grammar Errors",
                "db_column": "GRAMMAR_ERRORS",
                "db_table": "APP.LANGUAGE_ERROR",
            },
            {
                # APP.LANGUAGE_ERROR_COUNTS has no ENCODED_URL lookup key.
                "csv_column": "Error Count",
                "db_column": "ERROR_COUNT",
                "db_table": "APP.LANGUAGE_ERROR_COUNTS",
            },
        ]
    }

    table, entries, gui_defs, supplementary = _resolve_tab_entries(
        mapping, "spelling_and_grammar_errors", None
    )

    assert table == "APP.LANGUAGE_ERROR"
    assert gui_defs == []
    assert [entry["csv_column"] for entry in entries] == [
        "Spelling Errors",
        "Grammar Errors",
    ]
    assert supplementary == []


def test_build_supplementary_map_groups_by_table_and_keeps_first_csv_mapping() -> None:
    supplementary = [
        {
            "csv_column": "Performance Score",
            "db_column": "PERFORMANCE_SCORE",
            "db_table": "APP.PAGE_SPEED_API",
        },
        {
            "csv_column": "Total Requests",
            "db_column": "TOTAL_REQUESTS",
            "db_table": "APP.PAGE_SPEED_API",
        },
        {
            "csv_column": "Performance Score",
            "db_column": "OTHER_COL",
            "db_table": "APP.PAGE_SPEED_API",
        },
        {
            "csv_column": "Spelling Errors",
            "db_column": "SPELLING_ERRORS",
            "db_table": "APP.LANGUAGE_ERROR",
        },
    ]

    mapped = _build_supplementary_map(supplementary)

    assert mapped == {
        "APP.PAGE_SPEED_API": {
            "Performance Score": "PERFORMANCE_SCORE",
            "Total Requests": "TOTAL_REQUESTS",
        },
        "APP.LANGUAGE_ERROR": {"Spelling Errors": "SPELLING_ERRORS"},
    }


def test_build_where_from_entries_treats_null_literal_expressions_as_post_filters() -> None:
    where, params, post_filters = _build_where_from_entries(
        {"Extractor 1": None, "Source": "https://example.com/source"},
        [
            {
                "csv_column": "Source",
                "db_column": "ENCODED_URL",
                "db_table": "APP.LINKS",
            },
            {
                "csv_column": "Extractor 1",
                "db_expression": "NULL",
                "db_table": "APP.LINKS",
            },
        ],
    )

    assert where == "ENCODED_URL = ?"
    assert params == ["https://example.com/source"]
    assert post_filters == {"Extractor 1": None}


def test_fetch_existing_tables_reads_schema_names_from_sysschemas() -> None:
    cursor = _MetadataCursor([("APP.URLS",), ("APP.PAGE_SPEED_API",)])

    tables = _fetch_existing_tables(_MetadataConnection(cursor))

    assert "JOIN SYS.SYSSCHEMAS s ON t.SCHEMAID = s.SCHEMAID" in str(cursor.executed_sql)
    assert tables == frozenset({"APP.URLS", "APP.PAGE_SPEED_API"})
    assert cursor.closed is True


def test_fetch_existing_tables_handles_non_iterable_jaydebeapi_style_cursor() -> None:
    cursor = _NonIterableMetadataCursor([("APP.URLS",), ("APP.PAGE_SPEED_API",)])

    tables = _fetch_existing_tables(_MetadataConnection(cursor))

    assert tables == frozenset({"APP.URLS", "APP.PAGE_SPEED_API"})
    assert cursor.closed is True


def test_expression_references_absent_table_detects_joined_optional_tables() -> None:
    expr = (
        "SELECT p.SF_REQUEST_ERROR_KEY "
        "FROM APP.URLS u JOIN APP.PAGE_SPEED_API p ON p.ENCODED_URL = u.ENCODED_URL"
    )

    assert _expression_references_absent_table(expr, frozenset({"APP.URLS"})) is True
    assert _expression_references_absent_table(
        expr, frozenset({"APP.URLS", "APP.PAGE_SPEED_API"})
    ) is False


def test_expression_references_absent_column_detects_missing_joined_columns() -> None:
    expr = (
        "SELECT p.SF_REQUEST_ERROR_KEY "
        "FROM APP.URLS u JOIN APP.PAGE_SPEED_API p ON p.ENCODED_URL = u.ENCODED_URL"
    )
    known_columns = {
        "APP.URLS": frozenset({"ENCODED_URL"}),
        "APP.PAGE_SPEED_API": frozenset({"ENCODED_URL"}),
    }

    assert _expression_references_absent_column(expr, known_columns) is True


def test_expression_references_absent_column_detects_missing_default_table_columns() -> None:
    expr = (
        "CASE WHEN SF_REQUEST_ERROR_KEY IS NOT NULL AND SF_REQUEST_ERROR_KEY <> '' "
        "THEN SF_REQUEST_ERROR_KEY ELSE 'Success' END"
    )

    assert _expression_references_absent_column(
        expr,
        {"APP.PAGE_SPEED_API": frozenset({"ENCODED_URL"})},
        default_table="APP.PAGE_SPEED_API",
    ) is True
    assert _expression_references_absent_column(
        expr,
        {"APP.PAGE_SPEED_API": frozenset({"ENCODED_URL", "SF_REQUEST_ERROR_KEY"})},
        default_table="APP.PAGE_SPEED_API",
    ) is False


def test_compile_internal_filters_treats_unavailable_expression_fields_as_post_filters() -> None:
    where, params, post_filters = _compile_internal_filters(
        {
            "Address": "https://example.com/",
            "PSI Request Status": "Success",
        },
        {"Address": "ENCODED_URL"},
        [],
        {},
        {"psi_request_status"},
    )

    assert where == "ENCODED_URL = ?"
    assert params == ["https://example.com/"]
    assert post_filters == {"PSI Request Status": "Success"}


def test_build_where_from_entries_treats_absent_table_expressions_as_post_filters() -> None:
    where, params, post_filters = _build_where_from_entries(
        {"PSI Request Status": None, "Address": "https://example.com/source"},
        [
            {
                "csv_column": "Address",
                "db_column": "ENCODED_URL",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "PSI Request Status",
                "db_expression": (
                    "SELECT p.SF_REQUEST_ERROR_KEY FROM APP.PAGE_SPEED_API p "
                    "WHERE p.ENCODED_URL = APP.URLS.ENCODED_URL"
                ),
                "db_table": "APP.URLS",
            },
        ],
        existing_tables=frozenset({"APP.URLS"}),
    )

    assert where == "ENCODED_URL = ?"
    assert params == ["https://example.com/source"]
    assert post_filters == {"PSI Request Status": None}


def test_build_where_from_entries_treats_absent_column_expressions_as_post_filters() -> None:
    where, params, post_filters = _build_where_from_entries(
        {"PSI Request Status": "Success", "Address": "https://example.com/source"},
        [
            {
                "csv_column": "Address",
                "db_column": "ENCODED_URL",
                "db_table": "APP.PAGE_SPEED_API",
            },
            {
                "csv_column": "PSI Request Status",
                "db_expression": (
                    "CASE WHEN SF_REQUEST_ERROR_KEY IS NOT NULL "
                    "THEN SF_REQUEST_ERROR_KEY ELSE 'Success' END"
                ),
                "db_table": "APP.PAGE_SPEED_API",
            },
        ],
        existing_tables=frozenset({"APP.PAGE_SPEED_API"}),
        known_columns={"APP.PAGE_SPEED_API": frozenset({"ENCODED_URL"})},
    )

    assert where == "ENCODED_URL = ?"
    assert params == ["https://example.com/source"]
    assert post_filters == {"PSI Request Status": "Success"}


def test_build_where_from_entries_treats_absent_direct_columns_as_post_filters() -> None:
    where, params, post_filters = _build_where_from_entries(
        {"Viewport": "mobile", "Address": "https://example.com/source"},
        [
            {
                "csv_column": "Address",
                "db_column": "ENCODED_URL",
                "db_table": "APP.PAGE_SPEED_API",
            },
            {
                "csv_column": "Viewport",
                "db_column": "VIEWPORT",
                "db_table": "APP.PAGE_SPEED_API",
            },
        ],
        existing_tables=frozenset({"APP.PAGE_SPEED_API"}),
        known_columns={"APP.PAGE_SPEED_API": frozenset({"ENCODED_URL"})},
    )

    assert where == "ENCODED_URL = ?"
    assert params == ["https://example.com/source"]
    assert post_filters == {"Viewport": "mobile"}
