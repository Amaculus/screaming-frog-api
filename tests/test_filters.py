from __future__ import annotations

from screamingfrog.filters.registry import get_filter


def test_filter_registry_autoloads_definitions() -> None:
    filt = get_filter("Page Titles", "Missing")
    assert filt is not None
    assert filt.sql_where is not None


def test_internal_redirect_chain_filter_is_backed_by_sql() -> None:
    filt = get_filter("Response Codes", "Internal Redirect Chain")
    assert filt is not None
    assert filt.sql_where == "IS_INTERNAL = 1 AND IS_REDIRECT = 1 AND REDIRECT_COUNT > 0"


def test_hreflang_not_using_canonical_filter_is_backed_by_sql() -> None:
    filt = get_filter("Hreflang", "Not Using Canonical")
    assert filt is not None
    assert filt.sql_where is not None
    assert "APP.LINKS" in filt.sql_where
    assert "l.LINK_TYPE = 13" in filt.sql_where
    assert "u.IS_CANONICALISED = 1" in filt.sql_where


def test_hreflang_unlinked_filter_is_backed_by_sql() -> None:
    filt = get_filter("Hreflang", "Unlinked hreflang URLs")
    assert filt is not None
    assert filt.sql_where is not None
    assert "APP.LINKS.LINK_TYPE IN (12, 13)" in filt.sql_where
    assert "APP.INLINK_COUNTS" in filt.sql_where
    assert "APP.LINKS.DST_ID" in filt.sql_where


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


def test_pixel_filters_have_executable_row_predicates() -> None:
    title_over = get_filter("Page Titles", "Over X Pixels")
    title_below = get_filter("Page Titles", "Below X Pixels")
    meta_over = get_filter("Meta Description", "Over X Pixels")

    assert title_over is not None and title_over.row_predicate is not None
    assert title_below is not None and title_below.row_predicate is not None
    assert meta_over is not None and meta_over.row_predicate is not None
    assert title_over.row_predicate({"Title 1 Pixel Width": 562})
    assert not title_over.row_predicate({"Title 1 Pixel Width": 561})
    assert title_below.row_predicate({"Title 1 Pixel Width": 199})
    assert meta_over.row_predicate({"Meta Description 1 Pixel Width": 986})


def test_schema_backed_pagination_and_validation_filters_are_executable() -> None:
    for name in (
        "Non-200 Pagination URLs",
        "Unlinked Pagination URLs",
        "Non-Indexable",
    ):
        filt = get_filter("Pagination", name)
        assert filt is not None and filt.sql_where

    for name in ("Validation Errors", "Validation Warnings"):
        filt = get_filter("Structured Data", name)
        assert filt is not None and filt.sql_where
