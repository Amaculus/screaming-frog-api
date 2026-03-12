from __future__ import annotations

from screamingfrog.filters.registry import get_filter


def test_filter_registry_autoloads_definitions() -> None:
    filt = get_filter("Page Titles", "Missing")
    assert filt is not None
    assert filt.sql_where is not None


def test_structured_data_parse_errors_filter() -> None:
    filt = get_filter("Structured Data", "Parse Errors")
    assert filt is not None
    assert filt.sql_where is not None
    assert "PARSE_ERROR_MSG" in filt.sql_where
