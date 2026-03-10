from __future__ import annotations

from screamingfrog.filters.registry import get_filter


def test_filter_registry_autoloads_definitions() -> None:
    filt = get_filter("Page Titles", "Missing")
    assert filt is not None
    assert filt.sql_where is not None


def test_structured_data_contains_and_missing_use_blob_length() -> None:
    contains = get_filter("Structured Data", "Contains Structured Data")
    missing = get_filter("Structured Data", "Missing")

    assert contains is not None
    assert contains.sql_where is not None
    assert "LENGTH(SERIALISED_STRUCTURED_DATA) > 0" in contains.sql_where

    assert missing is not None
    assert missing.sql_where is not None
    assert "LENGTH(SERIALISED_STRUCTURED_DATA) = 0" in missing.sql_where


def test_structured_data_parse_errors_filter_uses_parse_error_column() -> None:
    parse_errors = get_filter("Structured Data", "Parse Errors")
    assert parse_errors is not None
    assert parse_errors.sql_where == "PARSE_ERROR_MSG IS NOT NULL AND PARSE_ERROR_MSG <> ''"


def test_structured_data_format_filters_define_blob_patterns() -> None:
    jsonld = get_filter("Structured Data", "JSON-LD URLs")
    microdata = get_filter("Structured Data", "Microdata URLs")
    rdfa = get_filter("Structured Data", "RDFa URLs")

    assert jsonld is not None and jsonld.blob_column == "SERIALISED_STRUCTURED_DATA"
    assert microdata is not None and microdata.blob_column == "SERIALISED_STRUCTURED_DATA"
    assert rdfa is not None and rdfa.blob_column == "SERIALISED_STRUCTURED_DATA"

    assert jsonld.blob_pattern == b"JSONLD"
    assert microdata.blob_pattern == b"MICRODATA"
    assert rdfa.blob_pattern == b"RDFA"
    assert jsonld.sql_where is not None and "LENGTH(SERIALISED_STRUCTURED_DATA) > 0" in jsonld.sql_where


def test_images_background_and_incorrect_size_filters_have_sql() -> None:
    background = get_filter("Images", "Background Images")
    incorrect_size = get_filter("Images", "Incorrectly Sized Images")

    assert background is not None
    assert background.sql_where is not None
    assert "l.LINK_TYPE = 23" in background.sql_where

    assert incorrect_size is not None
    assert incorrect_size.sql_where is not None
    assert "l.IMAGE_DISPLAY_WIDTH" in incorrect_size.sql_where
    assert "APP.URLS.IMAGE_WIDTH" in incorrect_size.sql_where
