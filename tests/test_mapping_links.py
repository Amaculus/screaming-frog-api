from __future__ import annotations

import json
from pathlib import Path


def test_all_inlinks_mapping_targets_links_table() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    entries = mapping.get("all_inlinks.csv", [])
    assert entries, "all_inlinks.csv mapping is missing"
    assert {entry.get("db_table") for entry in entries} == {"APP.LINKS"}
    columns = {entry.get("csv_column") for entry in entries}
    required = {
        "Type",
        "Source",
        "Destination",
        "Anchor",
        "Rel",
        "Follow",
        "Status Code",
        "Status",
        "Link Path",
        "Link Position",
        "hreflang",
        "Indexability",
        "Indexability Status",
    }
    assert required.issubset(columns)


def test_all_outlinks_mapping_targets_links_table() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    entries = mapping.get("all_outlinks.csv", [])
    assert entries, "all_outlinks.csv mapping is missing"
    assert {entry.get("db_table") for entry in entries} == {"APP.LINKS"}


def test_all_hreflang_urls_maps_link_destination_and_hreflang() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    entries = {entry["csv_column"]: entry for entry in mapping.get("all_hreflang_urls.csv", [])}

    assert entries["Source"]["db_table"] == "APP.LINKS"
    assert "APP.LINKS.SRC_ID" in entries["Source"]["db_expression"]
    assert entries["hreflang Alternate"]["db_table"] == "APP.LINKS"
    assert "APP.LINKS.DST_ID" in entries["hreflang Alternate"]["db_expression"]
    assert entries["hreflang"]["db_table"] == "APP.LINKS"
    assert entries["hreflang"]["db_column"] == "HREF_LANG"


def test_http_header_tabs_use_header_extracts() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))

    response_entries = mapping.get("all_http_response_headers.csv", [])
    for entry in response_entries:
        if entry["csv_column"] == "Address":
            continue
        if entry["csv_column"] == "content-type":
            assert entry["db_column"] == "CONTENT_TYPE"
            continue
        assert entry["db_column"] == "HTTP_RESPONSE_HEADER_COLLECTION"
        assert entry["header_extract"]["type"] == "header_name"
        assert entry["header_extract"]["column"] == "HTTP_RESPONSE_HEADER_COLLECTION"

    request_entries = mapping.get("all_http_request_headers.csv", [])
    for entry in request_entries:
        if entry["csv_column"] == "Address":
            continue
        assert entry["db_column"] == "HTTP_REQUEST_HEADER_COLLECTION"
        assert entry["header_extract"]["type"] == "header_name"
        assert entry["header_extract"]["column"] == "HTTP_REQUEST_HEADER_COLLECTION"


def test_status_column_uses_response_message_in_key_url_tabs() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))
    for tab_name in ("internal_all.csv", "security_all.csv", "external_all.csv"):
        entries = {entry["csv_column"]: entry for entry in mapping.get(tab_name, [])}
        assert entries["Status"]["db_table"] == "APP.URLS"
        assert entries["Status"]["db_column"] == "RESPONSE_MSG"


def test_cookie_and_language_tabs_use_blob_backed_mappings() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))

    cookie_entries = {entry["csv_column"]: entry for entry in mapping.get("all_cookies.csv", [])}
    assert cookie_entries["Cookie Name"]["db_column"] == "COOKIE_COLLECTION"
    assert cookie_entries["Cookie Name"]["blob_extract"]["type"] == "cookies"

    cookie_summary_entries = {
        entry["csv_column"]: entry for entry in mapping.get("cookie_summary.csv", [])
    }
    assert cookie_summary_entries["Occurrences"]["db_column"] == "COOKIE_COLLECTION"
    assert cookie_summary_entries["Occurrences"]["blob_extract"]["type"] == "cookie_summary"

    language_entries = {
        entry["csv_column"]: entry
        for entry in mapping.get("spelling_and_grammar_errors.csv", [])
    }
    assert language_entries["Error"]["db_column"] == "LANGUAGE_ERROR_DATA"
    assert language_entries["Error Count"]["blob_extract"]["type"] == "language_errors"

    language_summary_entries = {
        entry["csv_column"]: entry
        for entry in mapping.get("spelling_and_grammar_errors_report_summary.csv", [])
    }
    assert language_summary_entries["Coverage %"]["db_column"] == "LANGUAGE_ERROR_DATA"
    assert (
        language_summary_entries["Coverage %"]["blob_extract"]["type"]
        == "language_error_summary"
    )


def test_structured_data_tabs_use_blob_backed_or_direct_parse_error_mappings() -> None:
    mapping = json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))

    summary_entries = {
        entry["csv_column"]: entry
        for entry in mapping.get("structured_data_contains_structured_data.csv", [])
    }
    assert summary_entries["Feature-1"]["db_column"] == "SERIALISED_STRUCTURED_DATA"
    assert summary_entries["Feature-1"]["blob_extract"]["type"] == "structured_data"
    assert summary_entries["Type-1"]["db_column"] == "SERIALISED_STRUCTURED_DATA"
    assert summary_entries["Total Types"]["db_column"] == "SERIALISED_STRUCTURED_DATA"

    parse_error_entries = {
        entry["csv_column"]: entry
        for entry in mapping.get("structured_data_parse_error_report.csv", [])
    }
    assert parse_error_entries["Parse Error"]["db_column"] == "PARSE_ERROR_MSG"

    detailed_entries = {
        entry["csv_column"]: entry
        for entry in mapping.get("contains_structured_data_detailed_report.csv", [])
    }
    assert detailed_entries["Subject"]["db_column"] == "SERIALISED_STRUCTURED_DATA"
    assert (
        detailed_entries["Validation Type 1"]["blob_extract"]["type"]
        == "structured_data_detailed"
    )
