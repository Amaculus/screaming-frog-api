"""A 3xx row must never report itself as its own redirect destination.

Screaming Frog persists response headers only when the crawl enabled "Store
HTTP Headers", which is off by default. On a normal crawl
``HTTP_RESPONSE_HEADER_COLLECTION`` is therefore empty for every 3xx row and
``APP.URLS`` carries no destination column, so the ``redirect_url`` derived
extract fell through to ``urljoin(address, "")`` which returns *address*
verbatim. Every redirect rendered as ``A -> A``.

Measured on two real crawls before the fix: 47/47 and 79/79 3xx rows were
self-redirects, while ``APP.LINKS`` LINK_TYPE 15 held the true destination for
every one of them (six live HTTP cross-checks matched exactly).
"""

from __future__ import annotations

import pytest

from screamingfrog.backends.derby_backend import (
    REDIRECT_LINK_TYPE,
    _derived_extract_expressions,
    _extract_derived_value,
    _resolve_internal_alias_map,
)

_EXTRACT = {
    "type": "redirect_url",
    "columns": [
        "ENCODED_URL",
        "RESPONSE_CODE",
        "NUM_METAREFRESH",
        "META_FULL_URL_1",
        "META_FULL_URL_2",
        "HTTP_RESPONSE_HEADER_COLLECTION",
    ],
}
_SRC = "https://example.com/old/"


def _values(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "ENCODED_URL": _SRC,
        "RESPONSE_CODE": 301,
        "NUM_METAREFRESH": 0,
        "META_FULL_URL_1": None,
        "META_FULL_URL_2": None,
        "HTTP_RESPONSE_HEADER_COLLECTION": None,
    }
    base.update(overrides)
    return base


class TestNoSelfRedirect:
    def test_missing_destination_yields_none_not_the_source(self) -> None:
        """The regression that shipped: a blank target must not echo the source."""
        assert _extract_derived_value(_EXTRACT, _values()) is None

    def test_blank_link_target_yields_none(self) -> None:
        assert _extract_derived_value(_EXTRACT, _values(LINK_REDIRECT_TARGET="   ")) is None

    def test_non_redirect_status_ignores_the_link_edge(self) -> None:
        """A 200 has no destination even if a stray edge exists for the URL."""
        values = _values(RESPONSE_CODE=200, LINK_REDIRECT_TARGET="https://example.com/new/")
        assert _extract_derived_value(_EXTRACT, values) is None


class TestDestinationResolution:
    def test_link_edge_is_used_when_headers_are_absent(self) -> None:
        values = _values(LINK_REDIRECT_TARGET="https://example.com/new/")
        assert _extract_derived_value(_EXTRACT, values) == "https://example.com/new/"

    def test_relative_link_edge_is_absolutised_against_the_source(self) -> None:
        assert _extract_derived_value(_EXTRACT, _values(LINK_REDIRECT_TARGET="/new/")) == (
            "https://example.com/new/"
        )

    def test_meta_refresh_still_wins(self) -> None:
        values = _values(
            NUM_METAREFRESH=1,
            META_FULL_URL_1="/meta/",
            LINK_REDIRECT_TARGET="https://example.com/link/",
        )
        assert _extract_derived_value(_EXTRACT, values) == "https://example.com/meta/"

    def test_a_genuine_self_redirect_loop_is_reported_as_such(self) -> None:
        """Not every src == dst is the bug: a site can 301 a URL to itself.

        One such loop exists on a real crawl and is a critical finding, so the
        value must pass through rather than being suppressed.
        """
        values = _values(LINK_REDIRECT_TARGET=_SRC)
        assert _extract_derived_value(_EXTRACT, values) == _SRC


class TestMappingCarriesTheFallback:
    def test_every_redirect_url_extract_declares_the_link_expression(self) -> None:
        import json
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        for rel in ("schemas/mapping.json", "screamingfrog/resources/mapping.json"):
            mapping = json.loads((root / rel).read_text(encoding="utf-8"))
            checked = 0
            for entries in mapping.values():
                if not isinstance(entries, list):
                    continue
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    extract = entry.get("derived_extract") or {}
                    if extract.get("type") != "redirect_url":
                        continue
                    checked += 1
                    expressions = _derived_extract_expressions(entry)
                    assert "LINK_REDIRECT_TARGET" in expressions, f"{rel}: {entry['csv_column']}"
                    assert f"LINK_TYPE = {REDIRECT_LINK_TYPE}" in (
                        expressions["LINK_REDIRECT_TARGET"]
                    )
            assert checked, f"{rel}: no redirect_url extracts found"


class TestDerivedExtractExpressions:
    def test_non_dict_expressions_are_ignored(self) -> None:
        assert _derived_extract_expressions({"derived_extract": {"expressions": ["x"]}}) == {}

    def test_blank_alias_or_expression_is_dropped(self) -> None:
        entry = {"derived_extract": {"expressions": {"": "SELECT 1", "A": "  ", "B": "SELECT 2"}}}
        assert _derived_extract_expressions(entry) == {"B": "SELECT 2"}


class TestAliasMapNeverExposesExtractInputs:
    """An extract's ``db_column`` is its INPUT, never the column's value.

    ``_resolve_internal_alias_map`` skipped only ``header_extract`` entries, so
    every derived / blob / multi-row column was aliased straight to its own raw
    input. Measured on the real internal mapping: 908 columns, including
    "Redirect URL" -> ENCODED_URL (a self-redirect on all 34 internal 3xx rows
    of a real crawl), "Title 1 Pixel Width" -> the title text, and
    "Folder Depth" -> the URL.
    """

    _COLUMNS = ("ENCODED_URL", "TITLE_1", "CO2", "RESPONSE_CODE")

    @staticmethod
    def _entry(csv_column: str, db_column: str, **extract: object) -> dict[str, object]:
        return {
            "csv_column": csv_column,
            "db_column": db_column,
            "db_table": "APP.URLS",
            **extract,
        }

    def test_plain_columns_still_alias(self) -> None:
        mapping = {"internal_all.csv": [self._entry("Address", "ENCODED_URL")]}
        assert _resolve_internal_alias_map(mapping, "APP.URLS", self._COLUMNS) == {
            "Address": "ENCODED_URL"
        }

    @pytest.mark.parametrize(
        ("kind", "payload"),
        [
            ("derived_extract", {"type": "redirect_url"}),
            ("blob_extract", {"type": "cookie_count"}),
            ("multi_row_extract", {"type": "custom_extraction_match"}),
            ("header_extract", {"column": "HTTP_RESPONSE_HEADER_COLLECTION"}),
        ],
    )
    def test_every_extract_kind_is_excluded(self, kind: str, payload: dict) -> None:
        mapping = {
            "internal_all.csv": [self._entry("Redirect URL", "ENCODED_URL", **{kind: payload})]
        }
        assert _resolve_internal_alias_map(mapping, "APP.URLS", self._COLUMNS) == {}

    def test_the_real_mapping_only_aliases_url_columns_onto_the_url(self) -> None:
        """End-state check on the shipped mapping.

        Two columns legitimately read ENCODED_URL directly (the address and its
        URL-encoded form). Everything else that resolved to it did so only
        because it was an extract input, which is the bug.
        """
        import json
        from pathlib import Path

        root = Path(__file__).resolve().parents[1]
        mapping = json.loads((root / "schemas/mapping.json").read_text(encoding="utf-8"))
        columns = [
            str(e.get("db_column"))
            for e in mapping["internal_all.csv"]
            if isinstance(e, dict) and e.get("db_column")
        ]
        aliases = _resolve_internal_alias_map(mapping, "APP.URLS", columns)
        onto_url = sorted(csv for csv, db in aliases.items() if db == "ENCODED_URL")
        assert onto_url == ["Address", "URL Encoded Address"], (
            f"leaked columns aliased to the URL: {onto_url}"
        )
