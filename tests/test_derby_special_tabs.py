from __future__ import annotations

import gzip
import json
from typing import Any

from screamingfrog.backends.derby_backend import DerbyBackend, _cookie_expiration_text


class _FakeBlob:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def length(self) -> int:
        return len(self._data)

    def getBytes(self, start: int, length: int) -> bytes:
        offset = max(0, start - 1)
        return self._data[offset : offset + length]


class _FakeCursor:
    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.description = [(column,) for column in columns]
        self._rows = list(rows)
        self._fetch_index = 0
        self._fetchone_index = 0
        self.executed_sql: str | None = None
        self.executed_params: list[Any] | None = None

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        self.executed_sql = sql
        self.executed_params = list(params or [])

    def fetchmany(self, size: int) -> list[tuple[Any, ...]]:
        if self._fetch_index >= len(self._rows):
            return []
        end = min(self._fetch_index + size, len(self._rows))
        chunk = self._rows[self._fetch_index : end]
        self._fetch_index = end
        return chunk

    def fetchone(self) -> tuple[Any, ...] | None:
        if self._fetchone_index >= len(self._rows):
            return None
        row = self._rows[self._fetchone_index]
        self._fetchone_index += 1
        return row

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)


class _QueuedConnection:
    def __init__(self, cursors: list[_FakeCursor]) -> None:
        self._cursors = list(cursors)

    def cursor(self) -> _FakeCursor:
        if not self._cursors:
            raise AssertionError("No fake cursor queued")
        return self._cursors.pop(0)


def _cookie_blob(*cookies: dict[str, Any]) -> _FakeBlob:
    payload = {"mCookies": list(cookies)}
    return _FakeBlob(gzip.compress(json.dumps(payload).encode("utf-8")))


def _language_blob(errors: list[dict[str, Any]], *, lang: str = "en-US") -> _FakeBlob:
    payload = {
        "langCode": lang,
        "numSpellingErrors": sum(
            1
            for error in errors
            if str(error.get("errorType") or "").upper() in {"TYPO", "SPELLING", "MISSPELLING"}
        ),
        "numGrammarErrors": sum(
            1
            for error in errors
            if str(error.get("errorType") or "").upper() not in {"TYPO", "SPELLING", "MISSPELLING"}
        ),
        "errors": errors,
    }
    return _FakeBlob(gzip.compress(json.dumps(payload).encode("utf-8")))


def _structured_blob(format_name: str, text: str) -> _FakeBlob:
    raw = b"triplesDocumentFormat" + format_name.encode("ascii") + b"\x00\x00\x00\x00" + gzip.compress(
        text.encode("utf-8")
    )
    return _FakeBlob(raw)


def test_cookie_expiration_text_formats_seconds_and_sessions() -> None:
    assert _cookie_expiration_text(-1) == "Session"
    assert _cookie_expiration_text(1800) == "30 Minutes"
    assert _cookie_expiration_text(31535963) == "364 Days"


def test_get_tab_all_cookies_parses_cookie_collection_rows() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "COOKIE_COLLECTION"],
        [
            (
                "https://example.com/",
                _cookie_blob(
                    {
                        "mName": "sessionid",
                        "mValue": "abc",
                        "mDomain": "example.com",
                        "mPath": "/",
                        "mExpirationTime": -1,
                        "mIsSecure": True,
                        "mIsHttpOnly": False,
                    }
                ),
            )
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])
    backend._mapping = {
        "all_cookies.csv": [
            {"csv_column": "Address"},
            {"csv_column": "Cookie Name"},
            {"csv_column": "Cookie Value"},
            {"csv_column": "Domain"},
            {"csv_column": "Path"},
            {"csv_column": "Expiration Time"},
            {"csv_column": "Secure"},
            {"csv_column": "HttpOnly"},
        ]
    }

    rows = list(backend.get_tab("all_cookies"))

    assert rows == [
        {
            "Address": "https://example.com/",
            "Cookie Name": "sessionid",
            "Cookie Value": "abc",
            "Domain": "example.com",
            "Path": "/",
            "Expiration Time": "Session",
            "Secure": True,
            "HttpOnly": False,
        }
    ]


def test_get_tab_cookie_summary_aggregates_occurrences() -> None:
    cursor = _FakeCursor(
        ["ENCODED_URL", "COOKIE_COLLECTION"],
        [
            (
                "https://example.com/a",
                _cookie_blob(
                    {
                        "mName": "consent",
                        "mValue": "1",
                        "mDomain": "example.com",
                        "mPath": "/",
                        "mExpirationTime": 1800,
                        "mIsSecure": False,
                        "mIsHttpOnly": True,
                    }
                ),
            ),
            (
                "https://example.com/b",
                _cookie_blob(
                    {
                        "mName": "consent",
                        "mValue": "2",
                        "mDomain": "example.com",
                        "mPath": "/",
                        "mExpirationTime": 1800,
                        "mIsSecure": False,
                        "mIsHttpOnly": True,
                    }
                ),
            ),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])
    backend._mapping = {
        "cookie_summary.csv": [
            {"csv_column": "Cookie Name"},
            {"csv_column": "Domain"},
            {"csv_column": "Path"},
            {"csv_column": "Expiration Time"},
            {"csv_column": "Secure"},
            {"csv_column": "HttpOnly"},
            {"csv_column": "Occurrences"},
            {"csv_column": "Sample URL"},
        ]
    }

    rows = list(backend.get_tab("cookie_summary"))

    assert rows == [
        {
            "Cookie Name": "consent",
            "Domain": "example.com",
            "Path": "/",
            "Expiration Time": "30 Minutes",
            "Secure": False,
            "HttpOnly": True,
            "Occurrences": 2,
            "Sample URL": "https://example.com/a",
        }
    ]


def test_get_tab_internal_all_extracts_cookie_count_from_blob_mapping() -> None:
    cursor = _FakeCursor(
        ["COOKIE_COLLECTION"],
        [
            (
                _cookie_blob(
                    {"mName": "a", "mValue": "1"},
                    {"mName": "b", "mValue": "2"},
                    {"mName": "c", "mValue": "3"},
                ),
            )
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])
    backend._mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Cookies",
                "db_column": "COOKIE_COLLECTION",
                "db_table": "APP.URLS",
                "blob_extract": {"type": "cookie_count"},
            }
        ]
    }

    rows = list(backend.get_tab("internal_all"))

    assert rows == [{"Cookies": 3}]
    assert cursor.executed_sql == "SELECT COOKIE_COLLECTION FROM APP.URLS"


def test_get_tab_language_errors_groups_repeated_page_errors() -> None:
    errors = [
        {
            "ruleId": "MORFOLOGIK_RULE_EN_US",
            "errorType": "TYPO",
            "pageSection": "CONTENT",
            "error": "Possible spelling mistake found.",
            "suggestions": ["Benzinga"],
        },
        {
            "ruleId": "MORFOLOGIK_RULE_EN_US",
            "errorType": "TYPO",
            "pageSection": "CONTENT",
            "error": "Possible spelling mistake found.",
            "suggestions": ["Benzinga"],
        },
        {
            "ruleId": "UPPERCASE_SENTENCE_START",
            "errorType": "GRAMMAR",
            "pageSection": "TITLE",
            "error": "This sentence does not start with an uppercase letter.",
            "suggestions": [],
        },
    ]
    cursor = _FakeCursor(
        ["ENCODED_URL", "LANGUAGE_CODE", "SPELLING_ERRORS", "GRAMMAR_ERRORS", "LANGUAGE_ERROR_DATA"],
        [("https://example.com/", "en-US", 2, 1, _language_blob(errors))],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])
    backend._mapping = {
        "spelling_and_grammar_errors.csv": [
            {"csv_column": "URL"},
            {"csv_column": "Lang"},
            {"csv_column": "Spelling Errors"},
            {"csv_column": "Grammar Errors"},
            {"csv_column": "Error Type"},
            {"csv_column": "Error Count"},
            {"csv_column": "Error"},
            {"csv_column": "Error with Context"},
            {"csv_column": "Error Detail"},
            {"csv_column": "Suggestions"},
            {"csv_column": "Page Section"},
        ]
    }

    rows = list(backend.get_tab("spelling_and_grammar_errors"))

    assert rows == [
        {
            "URL": "https://example.com/",
            "Lang": "en-US",
            "Spelling Errors": 2,
            "Grammar Errors": 1,
            "Error Type": "Spelling",
            "Error Count": 2,
            "Error": "MORFOLOGIK_RULE_EN_US",
            "Error with Context": None,
            "Error Detail": "Possible spelling mistake found.",
            "Suggestions": "Benzinga",
            "Page Section": "Page Body",
        },
        {
            "URL": "https://example.com/",
            "Lang": "en-US",
            "Spelling Errors": 2,
            "Grammar Errors": 1,
            "Error Type": "Grammar",
            "Error Count": 1,
            "Error": "UPPERCASE_SENTENCE_START",
            "Error with Context": None,
            "Error Detail": "This sentence does not start with an uppercase letter.",
            "Suggestions": None,
            "Page Section": "Title",
        },
    ]


def test_get_tab_language_summary_aggregates_urls_and_counts() -> None:
    errors = [
        {
            "ruleId": "MORFOLOGIK_RULE_EN_US",
            "errorType": "TYPO",
            "pageSection": "CONTENT",
            "error": "Possible spelling mistake found.",
            "suggestions": ["Benzinga"],
        },
        {
            "ruleId": "MORFOLOGIK_RULE_EN_US",
            "errorType": "TYPO",
            "pageSection": "CONTENT",
            "error": "Possible spelling mistake found.",
            "suggestions": ["Benzinga"],
        },
    ]
    cursor = _FakeCursor(
        ["ENCODED_URL", "LANGUAGE_CODE", "SPELLING_ERRORS", "GRAMMAR_ERRORS", "LANGUAGE_ERROR_DATA"],
        [
            ("https://example.com/a", "en-US", 2, 0, _language_blob(errors)),
            ("https://example.com/b", "en-US", 2, 0, _language_blob(errors)),
        ],
    )
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])
    backend._mapping = {
        "spelling_and_grammar_errors_report_summary.csv": [
            {"csv_column": "Error"},
            {"csv_column": "Error Type"},
            {"csv_column": "Error Count"},
            {"csv_column": "URLs Affected"},
            {"csv_column": "Coverage %"},
            {"csv_column": "Error Detail"},
            {"csv_column": "Sample URL"},
        ]
    }

    rows = list(backend.get_tab("spelling_and_grammar_errors_report_summary"))

    assert rows == [
        {
            "Error": "MORFOLOGIK_RULE_EN_US",
            "Error Type": "Spelling",
            "Error Count": 4,
            "URLs Affected": 2,
            "Coverage %": 100.0,
            "Error Detail": "Possible spelling mistake found.",
            "Sample URL": "https://example.com/a",
        }
    ]


def test_get_tab_structured_data_summary_derives_formats_types_and_features() -> None:
    structured_text = (
        "_:b0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/FAQPage> .\n"
        "_:b1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Question> .\n"
        "_:b2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Answer> .\n"
    )
    data_cursor = _FakeCursor(
        [
            "ENCODED_URL",
            "SERIALISED_STRUCTURED_DATA",
            "PARSE_ERROR_MSG",
            "RICH_RESULTS_TYPES",
            "RICH_RESULTS_TYPE_ERRORS",
            "RICH_RESULTS_TYPE_WARNINGS",
        ],
        [
            (
                "https://example.com/faq",
                _structured_blob("JSONLD", structured_text),
                None,
                None,
                0,
                0,
            )
        ],
    )
    indexability_cursor = _FakeCursor(["IDX", "IDX_STATUS"], [("Indexable", None)])
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([data_cursor, indexability_cursor])
    backend._mapping = {
        "internal_all.csv": [
            {
                "csv_column": "Indexability",
                "db_expression": "CASE WHEN 1=1 THEN 'Indexable' END",
                "db_table": "APP.URLS",
            },
            {
                "csv_column": "Indexability Status",
                "db_expression": "CAST(NULL AS VARCHAR(1))",
                "db_table": "APP.URLS",
            },
        ],
        "structured_data_contains_structured_data.csv": [
            {"csv_column": "Address"},
            {"csv_column": "Errors"},
            {"csv_column": "Warnings"},
            {"csv_column": "Rich Result Errors"},
            {"csv_column": "Rich Result Warnings"},
            {"csv_column": "Rich Result Features"},
            {"csv_column": "Feature-1"},
            {"csv_column": "Total Types"},
            {"csv_column": "Unique Types"},
            {"csv_column": "Type-1"},
            {"csv_column": "Type-2"},
            {"csv_column": "Type-3"},
            {"csv_column": "Indexability"},
            {"csv_column": "Indexability Status"},
        ],
    }

    rows = list(backend.get_tab("structured_data_contains_structured_data"))

    assert rows == [
        {
            "Address": "https://example.com/faq",
            "Errors": 0,
            "Warnings": 0,
            "Rich Result Errors": 0,
            "Rich Result Warnings": 0,
            "Rich Result Features": 1,
            "Feature-1": "Google FAQ",
            "Total Types": 3,
            "Unique Types": 3,
            "Type-1": "FAQPage",
            "Type-2": "Question",
            "Type-3": "Answer",
            "Indexability": "Indexable",
            "Indexability Status": None,
        }
    ]


def test_pagespeed_audit_ids_cover_every_pagespeed_tab() -> None:
    from screamingfrog.backends.derby_backend import (
        _PAGESPEED_TAB_KEYS,
        PAGESPEED_AUDIT_IDS,
        PAGESPEED_PINNED_SPIDER_VERSION,
    )

    # every pagespeed tab resolves through a pinned mapping: the per-audit tabs
    # via PAGESPEED_AUDIT_IDS, the opportunities summary via its own label dict
    unmapped = _PAGESPEED_TAB_KEYS - set(PAGESPEED_AUDIT_IDS) - {
        "pagespeed_opportunities_summary.csv"
    }
    assert unmapped == set()
    assert PAGESPEED_PINNED_SPIDER_VERSION  # the pin must stay declared


def test_pagespeed_opportunity_specs_come_from_the_pinned_mapping() -> None:
    """The opportunity tab used to carry its own unpinned dict, so ten of its
    fifteen audit ids were outside the pin entirely (modern-image-formats,
    uses-optimized-images, uses-responsive-images, render-blocking-resources,
    unminified-css, unminified-javascript, uses-text-compression,
    uses-rel-preload, uses-rel-preconnect, redirects)."""
    from screamingfrog.backends.derby_backend import (
        PAGESPEED_OPPORTUNITY_AUDIT_IDS,
        _pagespeed_opportunity_specs,
    )

    assert _pagespeed_opportunity_specs() == PAGESPEED_OPPORTUNITY_AUDIT_IDS
    # A copy, so a caller mutating the result cannot corrupt the pin.
    _pagespeed_opportunity_specs()["Minify CSS"] = "bogus"
    assert PAGESPEED_OPPORTUNITY_AUDIT_IDS["Minify CSS"] == "unminified-css"


def test_pagespeed_opportunity_all_ids_missing_raises() -> None:
    """No pinned id present at all, with payloads to look in: the mapping is
    wholly stale, so this fails as loudly as the single-audit tabs do."""
    import pytest
    from screamingfrog.backends.derby_backend import (
        PageSpeedAuditMissingError,
        _check_pagespeed_audits_seen,
    )

    ids = ["unused-javascript", "modern-image-formats", "uses-responsive-images"]
    with pytest.raises(PageSpeedAuditMissingError) as exc:
        _check_pagespeed_audits_seen("pagespeed_opportunities_summary.csv", ids, 12, {})
    message = str(exc.value)
    # every missing id is named, not just the first: a Lighthouse consolidation
    # retires a whole family at once
    for audit_id in ids:
        assert audit_id in message
    assert "3 of 3" in message
    assert "22.2" in message


def test_pagespeed_opportunity_partial_miss_warns_rather_than_raising(caplog) -> None:
    """Some present, some absent. Almost certainly a retired audit, but not
    certainly: Screaming Frog lets the operator choose which PSI opportunities to
    store, so a partial ``audits`` object cannot be ruled out from here. Warning
    with the exact ids beats both a false alarm and silence."""
    import logging

    from screamingfrog.backends.derby_backend import _check_pagespeed_audits_seen

    ids = ["unused-javascript", "modern-image-formats", "uses-responsive-images"]
    with caplog.at_level(logging.WARNING):
        _check_pagespeed_audits_seen(
            "pagespeed_opportunities_summary.csv", ids, 12, {"unused-javascript": 12}
        )
    assert "modern-image-formats" in caplog.text
    assert "uses-responsive-images" in caplog.text
    assert "unused-javascript" not in caplog.text.split("payload(s): ")[1]


def test_pagespeed_opportunity_healthy_and_unconnected_are_both_silent(caplog) -> None:
    import logging

    from screamingfrog.backends.derby_backend import _check_pagespeed_audits_seen

    ids = ["unused-javascript", "modern-image-formats"]
    with caplog.at_level(logging.WARNING):
        # all ids seen -> healthy
        _check_pagespeed_audits_seen(
            "pagespeed_opportunities_summary.csv", ids, 12, dict.fromkeys(ids, 12)
        )
        # no payloads at all (PSI not connected) -> not a rename
        _check_pagespeed_audits_seen("pagespeed_opportunities_summary.csv", ids, 0, {})
    assert caplog.text == ""


def test_pagespeed_missing_audit_id_fails_loudly() -> None:
    import pytest
    from screamingfrog.backends.derby_backend import (
        PageSpeedAuditMissingError,
        _check_pagespeed_audit_seen,
    )

    # payloads exist but the audit id never appears -> rename/retire -> raise
    with pytest.raises(PageSpeedAuditMissingError) as exc:
        _check_pagespeed_audit_seen("defer_offscreen_images_report.csv", "offscreen-images", 12, 0)
    assert "offscreen-images" in str(exc.value)
    assert "22.2" in str(exc.value)

    # id present in at least one payload -> healthy
    _check_pagespeed_audit_seen("defer_offscreen_images_report.csv", "offscreen-images", 12, 12)
    # no payloads at all (PSI not connected / empty crawl) -> not a rename, no raise
    _check_pagespeed_audit_seen("defer_offscreen_images_report.csv", "offscreen-images", 0, 0)


def _psi_blob(audits: dict[str, Any]) -> _FakeBlob:
    payload = {"lighthouseResult": {"audits": audits}}
    return _FakeBlob(gzip.compress(json.dumps(payload).encode("utf-8")))


def test_opportunity_rows_raise_instead_of_emitting_a_table_of_zeros() -> None:
    """The symptom this guard exists for.

    The opportunities tab emits one row per label whatever happens, so a renamed
    audit produced a complete-looking table of fifteen zeros. A consumer could
    not tell that apart from a fast site, and would report the site as clean or
    blame the operator for not connecting PSI.
    """
    import pytest
    from screamingfrog.backends.derby_backend import PageSpeedAuditMissingError

    # A payload carrying audits, but under Lighthouse-13 style renamed ids.
    renamed = _psi_blob(
        {
            "improve-image-delivery": {"details": {"overallSavingsMs": 900}},
            "some-other-audit": {"details": {}},
        }
    )
    cursor = _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], [("https://x.test/", renamed)])
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])

    with pytest.raises(PageSpeedAuditMissingError) as exc:
        list(backend._iter_pagespeed_opportunity_rows({}))
    # Every pinned id is missing from this payload, so all fifteen are named.
    assert "15 of 15" in str(exc.value)
    assert "unused-javascript" in str(exc.value)


def test_opportunity_rows_still_emit_when_the_ids_are_present() -> None:
    """A present-but-passing audit is a healthy site, not a rename: the ids are
    in the payload, so nothing raises and the zero rows are real measurements."""
    from screamingfrog.backends.derby_backend import PAGESPEED_OPPORTUNITY_AUDIT_IDS

    passing = _psi_blob({aid: {"details": {}} for aid in PAGESPEED_OPPORTUNITY_AUDIT_IDS.values()})
    cursor = _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], [("https://x.test/", passing)])
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])

    rows = list(backend._iter_pagespeed_opportunity_rows({}))
    assert len(rows) == len(PAGESPEED_OPPORTUNITY_AUDIT_IDS)
    assert all(int(r["Number of URLs Affected"]) == 0 for r in rows)


def test_opportunity_rows_are_a_no_op_without_any_payloads() -> None:
    """PSI simply not connected: no payloads, no rename signal, no raise."""
    cursor = _FakeCursor(["ENCODED_URL", "JSON_RESPONSE"], [])
    backend = DerbyBackend.__new__(DerbyBackend)
    backend._conn = _QueuedConnection([cursor])
    rows = list(backend._iter_pagespeed_opportunity_rows({}))
    assert all(int(r["Number of URLs Affected"]) == 0 for r in rows)
