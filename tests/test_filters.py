from __future__ import annotations

from screamingfrog.filters.registry import get_filter


def test_filter_registry_autoloads_definitions() -> None:
    filt = get_filter("Page Titles", "Missing")
    assert filt is not None
    assert filt.sql_where is not None
