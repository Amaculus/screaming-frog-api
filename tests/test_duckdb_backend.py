from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from screamingfrog import Crawl
from screamingfrog.backends.base import CrawlBackend
from screamingfrog.models import InternalPage


class FakeDuckExportBackend(CrawlBackend):
    def __init__(self) -> None:
        self._internal_rows = [
            {"Address": "https://example.com/ok", "Status Code": 200, "Title 1": "OK"},
            {"Address": "https://example.com/broken", "Status Code": 404, "Title 1": ""},
        ]
        self._tabs = {
            "internal_all.csv": list(self._internal_rows),
            "response_codes_internal_client_error_(4xx).csv": [
                {"Address": "https://example.com/broken", "Status Code": 404}
            ],
            "all_inlinks.csv": [
                {
                    "Address": "https://example.com/broken",
                    "Source": "https://example.com/source",
                    "Anchor": "Broken link",
                    "Status Code": 404,
                }
            ],
            "all_outlinks.csv": [
                {
                    "Source": "https://example.com/source",
                    "Destination": "https://example.com/broken",
                    "Anchor": "Broken link",
                    "Status Code": 404,
                }
            ],
            "redirect_chains.csv": [
                {
                    "Address": "https://example.com/redirect",
                    "Number of Redirects": 4,
                    "Loop": False,
                }
            ],
            "canonical_chains.csv": [],
            "redirect_and_canonical_chains.csv": [],
        }
        self._raw = {
            "APP.URLS": [
                {"ID": 1, "ENCODED_URL": "https://example.com/ok", "RESPONSE_CODE": 200},
                {"ID": 2, "ENCODED_URL": "https://example.com/broken", "RESPONSE_CODE": 404},
            ],
            "APP.UNIQUE_URLS": [
                {"ID": 1, "ENCODED_URL": "https://example.com/source"},
                {"ID": 2, "ENCODED_URL": "https://example.com/broken"},
            ],
            "APP.LINKS": [
                {"SRC_ID": 1, "DST_ID": 2, "LINK_TEXT": "Broken link"},
            ],
        }

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        for row in self._internal_rows:
            yield InternalPage.from_data(row)

    def get_inlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return len(self._internal_rows)

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def list_tabs(self) -> list[str]:
        return sorted(self._tabs.keys())

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        name = tab_name if str(tab_name).endswith(".csv") else f"{tab_name}.csv"
        for row in self._tabs.get(name, []):
            yield dict(row)

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        for row in self._raw.get(str(table).upper(), []):
            yield dict(row)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        raise NotImplementedError


class MinimalDuckReportBackend(CrawlBackend):
    def __init__(self) -> None:
        self._tabs = {
            "internal_all.csv": [
                {
                    "Address": "https://example.com/home",
                    "Status Code": 200,
                    "Title 1": "Home",
                    "Meta Description 1": "Welcome home",
                    "Indexability": "Indexable",
                    "Indexability Status": "Indexable",
                },
                {
                    "Address": "https://example.com/orphan",
                    "Status Code": 200,
                    "Title 1": "Orphan",
                    "Meta Description 1": "",
                    "Indexability": "Indexable",
                    "Indexability Status": "Indexable",
                },
                {
                    "Address": "https://example.com/noindex-orphan",
                    "Status Code": 200,
                    "Title 1": "",
                    "Meta Description 1": "",
                    "Indexability": "Non-Indexable",
                    "Indexability Status": "Noindex",
                },
                {
                    "Address": "https://example.com/broken-page",
                    "Status Code": 404,
                    "Title 1": "Broken Page",
                    "Meta Description 1": "Broken page",
                    "Indexability": "Indexable",
                    "Indexability Status": "Indexable",
                },
            ]
        }
        self._raw = {
            "APP.URLS": [
                {"ENCODED_URL": "https://example.com/home", "RESPONSE_CODE": 200, "RESPONSE_MSG": "OK"},
                {"ENCODED_URL": "https://example.com/orphan", "RESPONSE_CODE": 200, "RESPONSE_MSG": "OK"},
                {"ENCODED_URL": "https://example.com/noindex-orphan", "RESPONSE_CODE": 200, "RESPONSE_MSG": "OK"},
                {"ENCODED_URL": "https://example.com/broken-page", "RESPONSE_CODE": 404, "RESPONSE_MSG": "Not Found"},
                {"ENCODED_URL": "https://example.com/broken-target", "RESPONSE_CODE": 404, "RESPONSE_MSG": "Not Found"},
                {"ENCODED_URL": "https://example.com/sponsored", "RESPONSE_CODE": 200, "RESPONSE_MSG": "OK"},
            ],
            "APP.UNIQUE_URLS": [
                {"ID": 1, "ENCODED_URL": "https://example.com/nav"},
                {"ID": 2, "ENCODED_URL": "https://example.com/home"},
                {"ID": 3, "ENCODED_URL": "https://example.com/broken-target"},
                {"ID": 4, "ENCODED_URL": "https://example.com/sponsored"},
                {"ID": 5, "ENCODED_URL": "https://example.com/orphan"},
                {"ID": 6, "ENCODED_URL": "https://example.com/noindex-orphan"},
                {"ID": 7, "ENCODED_URL": "https://example.com/broken-page"},
            ],
            "APP.LINKS": [
                {
                    "SRC_ID": 1,
                    "DST_ID": 2,
                    "ALT_TEXT": None,
                    "LINK_TEXT": "Home",
                    "HREF_LANG": None,
                    "NOFOLLOW": False,
                    "UGC": False,
                    "SPONSORED": False,
                    "TARGET": None,
                    "NOOPENER": False,
                    "NOREFERRER": False,
                    "PATH_TYPE": "html",
                    "ELEMENT_PATH": "a.nav",
                    "ELEMENT_POSITION": 1,
                    "LINK_TYPE": 1,
                    "SCOPE": 0,
                    "ORIGIN": 1,
                },
                {
                    "SRC_ID": 1,
                    "DST_ID": 3,
                    "ALT_TEXT": None,
                    "LINK_TEXT": "Broken",
                    "HREF_LANG": None,
                    "NOFOLLOW": False,
                    "UGC": False,
                    "SPONSORED": False,
                    "TARGET": None,
                    "NOOPENER": False,
                    "NOREFERRER": False,
                    "PATH_TYPE": "html",
                    "ELEMENT_PATH": "a.broken",
                    "ELEMENT_POSITION": 2,
                    "LINK_TYPE": 1,
                    "SCOPE": 0,
                    "ORIGIN": 1,
                },
                {
                    "SRC_ID": 1,
                    "DST_ID": 7,
                    "ALT_TEXT": None,
                    "LINK_TEXT": "Broken internal",
                    "HREF_LANG": None,
                    "NOFOLLOW": False,
                    "UGC": False,
                    "SPONSORED": False,
                    "TARGET": None,
                    "NOOPENER": False,
                    "NOREFERRER": False,
                    "PATH_TYPE": "html",
                    "ELEMENT_PATH": "a.broken-internal",
                    "ELEMENT_POSITION": 4,
                    "LINK_TYPE": 1,
                    "SCOPE": 0,
                    "ORIGIN": 1,
                },
                {
                    "SRC_ID": 1,
                    "DST_ID": 4,
                    "ALT_TEXT": None,
                    "LINK_TEXT": "Sponsored",
                    "HREF_LANG": None,
                    "NOFOLLOW": True,
                    "UGC": False,
                    "SPONSORED": True,
                    "TARGET": None,
                    "NOOPENER": False,
                    "NOREFERRER": False,
                    "PATH_TYPE": "html",
                    "ELEMENT_PATH": "a.sponsored",
                    "ELEMENT_POSITION": 3,
                    "LINK_TYPE": 1,
                    "SCOPE": 0,
                    "ORIGIN": 1,
                },
            ],
        }

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        for row in self._tabs["internal_all.csv"]:
            yield InternalPage.from_data(row)

    def get_inlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return len(self._tabs["internal_all.csv"])

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def list_tabs(self) -> list[str]:
        return sorted(self._tabs.keys())

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        name = tab_name if str(tab_name).endswith(".csv") else f"{tab_name}.csv"
        for row in self._tabs.get(name, []):
            yield dict(row)

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        for row in self._raw.get(str(table).upper(), []):
            yield dict(row)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        raise NotImplementedError


class FakeDuckCompareBackend(CrawlBackend):
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._tabs = {"internal_all.csv": rows}
        self._raw = {
            "APP.URLS": [
                {
                    "ENCODED_URL": row["Address"],
                    "RESPONSE_CODE": row["Status Code"],
                    "RESPONSE_MSG": "OK" if row["Status Code"] == 200 else "Moved Permanently",
                }
                for row in rows
            ],
            "APP.UNIQUE_URLS": [
                {"ID": index + 1, "ENCODED_URL": row["Address"]} for index, row in enumerate(rows)
            ],
            "APP.LINKS": [],
        }

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        for row in self._tabs["internal_all.csv"]:
            yield InternalPage.from_data(row)

    def get_inlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return len(self._tabs["internal_all.csv"])

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def list_tabs(self) -> list[str]:
        return sorted(self._tabs.keys())

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        name = tab_name if str(tab_name).endswith(".csv") else f"{tab_name}.csv"
        for row in self._tabs.get(name, []):
            yield dict(row)

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        for row in self._raw.get(str(table).upper(), []):
            yield dict(row)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        raise NotImplementedError


class IssueDuckBackend(CrawlBackend):
    def __init__(self) -> None:
        self._tabs = {
            "internal_all.csv": [
                {
                    "Address": "https://example.com/home",
                    "Status Code": 200,
                    "Title 1": "Home",
                }
            ],
            "security_missing_hsts_header.csv": [
                {"Address": "https://example.com/home", "Status Code": 200}
            ],
            "canonicals_missing.csv": [
                {"Address": "https://example.com/canonical", "Status Code": 200}
            ],
            "hreflang_missing_return_links.csv": [
                {"Address": "https://example.com/hreflang", "Status Code": 200}
            ],
            "response_codes_internal_redirect_chain.csv": [
                {"Address": "https://example.com/redirect", "Status Code": 301}
            ],
        }
        self._raw = {
            "APP.URLS": [
                {"ENCODED_URL": "https://example.com/home", "RESPONSE_CODE": 200, "RESPONSE_MSG": "OK"}
            ],
            "APP.UNIQUE_URLS": [{"ID": 1, "ENCODED_URL": "https://example.com/home"}],
            "APP.LINKS": [],
        }

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        for row in self._tabs["internal_all.csv"]:
            yield InternalPage.from_data(row)

    def get_inlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return len(self._tabs["internal_all.csv"])

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def list_tabs(self) -> list[str]:
        return sorted(self._tabs.keys())

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        name = tab_name if str(tab_name).endswith(".csv") else f"{tab_name}.csv"
        for row in self._tabs.get(name, []):
            yield dict(row)

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        for row in self._raw.get(str(table).upper(), []):
            yield dict(row)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        raise NotImplementedError


def test_export_and_load_duckdb_cache(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl.duckdb"

    exported = crawl.export_duckdb(str(target), source_label="fake-crawl")
    duck = Crawl.from_duckdb(str(exported))

    assert exported.exists()
    assert "internal_all.csv" in duck.tabs
    assert duck.pages().filter(status_code=404).collect() == [
        {"Address": "https://example.com/broken", "Status Code": 404, "Title 1": ""}
    ]
    assert next(duck.raw("APP.URLS"))["ENCODED_URL"] == "https://example.com/ok"
    assert (
        duck.query("APP", "URLS")
        .select("ENCODED_URL", "RESPONSE_CODE")
        .where("RESPONSE_CODE >= ?", 400)
        .collect()
        == [{"ENCODED_URL": "https://example.com/broken", "RESPONSE_CODE": 404}]
    )


def test_duckdb_backend_supports_links_and_chain_reports(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl.duckdb"
    crawl.export_duckdb(str(target), source_label="fake-crawl")

    duck = Crawl.load(str(target))

    inlinks = list(duck.inlinks("https://example.com/broken"))
    outlinks = list(duck.outlinks("https://example.com/source"))
    chains = duck.redirect_chain_report(min_hops=3)

    assert inlinks[0].source == "https://example.com/source"
    assert inlinks[0].destination == "https://example.com/broken"
    assert outlinks[0].destination == "https://example.com/broken"
    assert chains == [
        {"Address": "https://example.com/redirect", "Number of Redirects": 4, "Loop": False}
    ]


def test_export_duckdb_can_materialize_all_available_tabs(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-all.duckdb"

    crawl.export_duckdb(str(target), source_label="fake-crawl", tabs="all")
    duck = Crawl.from_duckdb(str(target))

    rows = duck.tab("response_codes_internal_client_error_(4xx)").collect()

    assert "response_codes_internal_client_error_(4xx).csv" in duck.tabs
    assert rows == [{"Address": "https://example.com/broken", "Status Code": 404}]


def test_duckdb_backend_resolves_gui_filters_and_base_all_tabs(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-all.duckdb"

    crawl.export_duckdb(str(target), source_label="fake-crawl", tabs="all")
    duck = Crawl.from_duckdb(str(target))

    filtered = duck.tab("Response Codes").filter(gui="Internal Client Error (4xx)").collect()
    internal_rows = duck.tab("Internal").collect()
    internal_columns = duck.tab_columns("Internal")

    assert filtered == [{"Address": "https://example.com/broken", "Status Code": 404}]
    assert internal_rows == [
        {"Address": "https://example.com/ok", "Status Code": 200, "Title 1": "OK"},
        {"Address": "https://example.com/broken", "Status Code": 404, "Title 1": ""},
    ]
    assert internal_columns == ["Address", "Status Code", "Title 1"]


def test_export_duckdb_auto_reuses_matching_source_and_refreshes_on_change(tmp_path: Path) -> None:
    source = tmp_path / "crawl.dbseospider"
    source.write_text("v1", encoding="utf-8")

    backend = FakeDuckExportBackend()
    backend.db_path = source
    crawl = Crawl(backend)
    target = tmp_path / "crawl.duckdb"

    exported = crawl.export_duckdb(str(target), source_label="fake-crawl", if_exists="auto")

    import duckdb

    conn = duckdb.connect(str(exported), read_only=True)
    first = conn.execute(
        "SELECT source_fingerprint, imported_at FROM sf_alpha_imports LIMIT 1"
    ).fetchone()
    conn.close()

    exported = crawl.export_duckdb(str(target), source_label="fake-crawl", if_exists="auto")
    conn = duckdb.connect(str(exported), read_only=True)
    second = conn.execute(
        "SELECT source_fingerprint, imported_at FROM sf_alpha_imports LIMIT 1"
    ).fetchone()
    conn.close()

    assert second == first

    source.write_text("v2-with-change", encoding="utf-8")
    os.utime(source, None)
    exported = crawl.export_duckdb(str(target), source_label="fake-crawl", if_exists="auto")
    conn = duckdb.connect(str(exported), read_only=True)
    third = conn.execute(
        "SELECT source_fingerprint, imported_at FROM sf_alpha_imports LIMIT 1"
    ).fetchone()
    conn.close()

    assert third[0] != first[0]


def test_duckdb_report_helpers_work_without_materialized_link_tabs(tmp_path: Path) -> None:
    crawl = Crawl(MinimalDuckReportBackend())
    target = tmp_path / "crawl-minimal.duckdb"

    crawl.export_duckdb(str(target), source_label="minimal", tabs=("internal_all",))
    duck = Crawl.from_duckdb(str(target))

    broken = duck.broken_inlinks_report()
    broken_pages = duck.broken_links_report()
    inlinks = list(duck.inlinks("https://example.com/broken-page"))
    outlinks = list(duck.outlinks("https://example.com/nav"))
    title_meta = duck.title_meta_audit()
    non_indexable = duck.indexability_audit()
    nofollow = duck.nofollow_inlinks_report()
    orphans = duck.orphan_pages_report()
    indexable_orphans = duck.orphan_pages_report(only_indexable=True)
    summary = duck.summary()

    assert broken == [
        {
            "Type": "Hyperlink",
            "Source": "https://example.com/nav",
            "Address": "https://example.com/broken-page",
            "Destination": "https://example.com/broken-page",
            "Alt Text": None,
            "Anchor": "Broken internal",
            "Status Code": 404,
            "Status": "Not Found",
            "Follow": True,
            "Target": None,
            "Rel": None,
            "Path Type": "html",
            "Link Path": "a.broken-internal",
            "Link Position": 4,
            "hreflang": None,
            "Link Type": 1,
            "Scope": 0,
            "Origin": 1,
            "NoFollow": False,
            "UGC": False,
            "Sponsored": False,
            "Noopener": False,
            "Noreferrer": False,
        },
        {
            "Type": "Hyperlink",
            "Source": "https://example.com/nav",
            "Address": "https://example.com/broken-target",
            "Destination": "https://example.com/broken-target",
            "Alt Text": None,
            "Anchor": "Broken",
            "Status Code": 404,
            "Status": "Not Found",
            "Follow": True,
            "Target": None,
            "Rel": None,
            "Path Type": "html",
            "Link Path": "a.broken",
            "Link Position": 2,
            "hreflang": None,
            "Link Type": 1,
            "Scope": 0,
            "Origin": 1,
            "NoFollow": False,
            "UGC": False,
            "Sponsored": False,
            "Noopener": False,
            "Noreferrer": False,
        },
    ]
    assert nofollow == [
        {
            "Type": "Hyperlink",
            "Source": "https://example.com/nav",
            "Address": "https://example.com/sponsored",
            "Destination": "https://example.com/sponsored",
            "Alt Text": None,
            "Anchor": "Sponsored",
            "Status Code": 200,
            "Status": "OK",
            "Follow": False,
            "Target": None,
            "Rel": "nofollow sponsored",
            "Path Type": "html",
            "Link Path": "a.sponsored",
            "Link Position": 3,
            "hreflang": None,
            "Link Type": 1,
            "Scope": 0,
            "Origin": 1,
            "NoFollow": True,
            "UGC": False,
            "Sponsored": True,
            "Noopener": False,
            "Noreferrer": False,
        }
    ]
    assert broken_pages == [
        {
            "Address": "https://example.com/broken-page",
            "Status Code": 404,
            "Inlinks": 1,
            "Inlink Sources": ["https://example.com/nav"],
            "Inlink Anchors": ["Broken internal"],
        }
    ]
    assert [(link.source, link.destination, link.anchor_text) for link in inlinks] == [
        ("https://example.com/nav", "https://example.com/broken-page", "Broken internal")
    ]
    assert [(link.source, link.destination, link.anchor_text) for link in outlinks] == [
        ("https://example.com/nav", "https://example.com/broken-page", "Broken internal"),
        ("https://example.com/nav", "https://example.com/broken-target", "Broken"),
        ("https://example.com/nav", "https://example.com/home", "Home"),
        ("https://example.com/nav", "https://example.com/sponsored", "Sponsored"),
    ]
    assert title_meta == [
        {"Address": "https://example.com/noindex-orphan", "Issue": "Missing Title"},
        {"Address": "https://example.com/noindex-orphan", "Issue": "Missing Meta Description"},
        {"Address": "https://example.com/orphan", "Issue": "Missing Meta Description"},
    ]
    assert non_indexable == [
        {
            "Address": "https://example.com/noindex-orphan",
            "Status Code": 200,
            "Indexability": "Non-Indexable",
            "Indexability Status": "Noindex",
            "Canonical": None,
            "Meta Robots": None,
            "X-Robots-Tag": None,
        }
    ]
    assert [row["Address"] for row in orphans] == [
        "https://example.com/orphan",
        "https://example.com/noindex-orphan",
    ]
    assert [row["Address"] for row in indexable_orphans] == ["https://example.com/orphan"]
    assert summary == {
        "pages": 4,
        "tabs": 1,
        "broken_pages": 1,
        "broken_inlinks": 2,
        "nofollow_inlinks": 1,
        "orphan_pages": 2,
        "non_indexable_pages": 1,
        "redirect_chains": 0,
        "security_issues": 0,
        "canonical_issues": 0,
        "hreflang_issues": 0,
        "redirect_issues": 0,
    }


def test_duckdb_issue_helpers_read_issue_tabs_directly(tmp_path: Path) -> None:
    crawl = Crawl(IssueDuckBackend())
    target = tmp_path / "crawl-issues.duckdb"

    crawl.export_duckdb(str(target), source_label="issues", tabs="all")
    duck = Crawl.from_duckdb(str(target))

    assert duck.security_issues_report() == [
        {
            "Address": "https://example.com/home",
            "Status Code": 200,
            "Issue": "Missing HSTS Header",
        }
    ]
    assert duck.canonical_issues_report() == [
        {
            "Address": "https://example.com/canonical",
            "Status Code": 200,
            "Issue": "Missing Canonical",
        }
    ]
    assert duck.hreflang_issues_report() == [
        {
            "Address": "https://example.com/hreflang",
            "Status Code": 200,
            "Issue": "Missing Return Links",
        }
    ]
    assert duck.redirect_issues_report() == [
        {
            "Address": "https://example.com/redirect",
            "Status Code": 301,
            "Issue": "Redirect Chain",
        }
    ]


def test_duckdb_compare_uses_projected_internal_rows(tmp_path: Path) -> None:
    old_rows = [
        {
            "Address": "https://example.com/home",
            "Status Code": 200,
            "Title 1": "Home",
            "Redirect URL": None,
            "Redirect Type": None,
            "Canonical Link Element 1": None,
            "Indexability": "Indexable",
            "Indexability Status": "Indexable",
            "Meta Robots 1": None,
            "X-Robots-Tag 1": None,
        },
        {
            "Address": "https://example.com/removed",
            "Status Code": 200,
            "Title 1": "Removed",
            "Redirect URL": None,
            "Redirect Type": None,
            "Canonical Link Element 1": None,
            "Indexability": "Indexable",
            "Indexability Status": "Indexable",
            "Meta Robots 1": None,
            "X-Robots-Tag 1": None,
        },
    ]
    new_rows = [
        {
            "Address": "https://example.com/home",
            "Status Code": 301,
            "Title 1": "Homepage",
            "Redirect URL": "https://example.com/new-home",
            "Redirect Type": "HTTP Redirect",
            "Canonical Link Element 1": None,
            "Indexability": "Indexable",
            "Indexability Status": "Indexable",
            "Meta Robots 1": None,
            "X-Robots-Tag 1": None,
        },
        {
            "Address": "https://example.com/added",
            "Status Code": 200,
            "Title 1": "Added",
            "Redirect URL": None,
            "Redirect Type": None,
            "Canonical Link Element 1": None,
            "Indexability": "Indexable",
            "Indexability Status": "Indexable",
            "Meta Robots 1": None,
            "X-Robots-Tag 1": None,
        },
    ]

    old_duckdb = tmp_path / "old.duckdb"
    new_duckdb = tmp_path / "new.duckdb"
    Crawl(FakeDuckCompareBackend(old_rows)).export_duckdb(str(old_duckdb), tabs=("internal_all",))
    Crawl(FakeDuckCompareBackend(new_rows)).export_duckdb(str(new_duckdb), tabs=("internal_all",))

    old_crawl = Crawl.from_duckdb(str(old_duckdb))
    new_crawl = Crawl.from_duckdb(str(new_duckdb))

    diff = new_crawl.compare(old_crawl)

    assert diff.added_pages == ["https://example.com/added"]
    assert diff.removed_pages == ["https://example.com/removed"]
    assert [(change.url, change.old_status, change.new_status) for change in diff.status_changes] == [
        ("https://example.com/home", 200, 301)
    ]
    assert [(change.url, change.old_title, change.new_title) for change in diff.title_changes] == [
        ("https://example.com/home", "Home", "Homepage")
    ]
    assert [
        (
            change.url,
            change.old_target,
            change.new_target,
            change.old_type,
            change.new_type,
        )
        for change in diff.redirect_changes
    ] == [
        (
            "https://example.com/home",
            None,
            "https://example.com/new-home",
            None,
            "HTTP Redirect",
        )
    ]
