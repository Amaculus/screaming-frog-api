from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

import pytest

from screamingfrog import Crawl
from screamingfrog.backends.base import CrawlBackend
from screamingfrog.db.duckdb import (
    _bulk_load_syscs_csvs_to_duckdb,
    _helper_relation_name,
    _import_duckdb,
    _relation_exists,
    iter_relation_rows,
    ensure_duckdb_cache,
    export_duckdb_from_backend,
    resolve_relation_name,
)
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

    def iter_link_projection(
        self,
        direction: str,
        fields: Sequence[str],
        filters: Optional[dict[str, Any]] = None,
    ) -> Iterator[dict[str, Any]]:
        id_to_url = {row["ID"]: row["ENCODED_URL"] for row in self._raw["APP.UNIQUE_URLS"]}
        url_to_status = {
            row["ENCODED_URL"]: row["RESPONSE_CODE"]
            for row in self._raw["APP.URLS"]
        }
        for row in self._raw["APP.LINKS"]:
            shaped = {
                "Source": id_to_url[row["SRC_ID"]],
                "Address": id_to_url[row["DST_ID"]],
                "Destination": id_to_url[row["DST_ID"]],
                "Anchor": row["LINK_TEXT"],
                "Status Code": url_to_status.get(id_to_url[row["DST_ID"]]),
            }
            if filters and filters.get("status_code") not in (None, shaped["Status Code"]):
                continue
            yield {field: shaped.get(field) for field in fields}

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

    def tab_columns(self, tab_name: str) -> list[str]:
        name = tab_name if str(tab_name).endswith(".csv") else f"{tab_name}.csv"
        rows = self._tabs.get(name, [])
        if not rows:
            return []
        return list(rows[0].keys())

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

    def iter_internal_projection(
        self, fields: Sequence[str], filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        wanted = tuple(str(field) for field in fields)
        for row in self._tabs["internal_all.csv"]:
            yield {field: row.get(field) for field in wanted}

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


class ProjectedOnlyCompareBackend(FakeDuckCompareBackend):
    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        raise AssertionError("compare() should use iter_internal_projection() on lean caches")


class ProjectedOnlyResponseCodesBackend(CrawlBackend):
    def __init__(self) -> None:
        self._rows = [
            {
                "Address": "https://example.com/ok",
                "Content Type": "text/html",
                "Status Code": 200,
                "Status": "OK",
                "Indexability": "Indexable",
                "Indexability Status": None,
                "Inlinks": 4,
                "Response Time": 123,
                "Redirect URL": None,
                "Redirect Type": None,
            },
            {
                "Address": "https://example.com/redirect",
                "Content Type": "text/html",
                "Status Code": 301,
                "Status": "Moved Permanently",
                "Indexability": "Indexable",
                "Indexability Status": None,
                "Inlinks": 2,
                "Response Time": 45,
                "Redirect URL": "https://example.com/final",
                "Redirect Type": "HTTP Redirect",
            },
        ]

    def iter_internal_projection(
        self, fields: Sequence[str], filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        wanted = tuple(str(field) for field in fields)
        for row in self._rows:
            yield {field: row.get(field) for field in wanted}

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        raise AssertionError("response_codes_all fast path should avoid get_internal()")

    def get_inlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return len(self._rows)

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def list_tabs(self) -> list[str]:
        return ["response_codes_all.csv"]

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        raise AssertionError("response_codes_all should use the projected DuckDB fast path")

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        return iter(())

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        return iter(())


class ProjectedOnlyInternalBackend(CrawlBackend):
    def __init__(self) -> None:
        self._rows = [
            {
                "Address": "https://example.com/home",
                "Status Code": 200,
                "Title 1": "Home",
                "Meta Description 1": "Desc",
            }
        ]

    def iter_internal_projection(
        self, fields: Sequence[str], filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        wanted = tuple(str(field) for field in fields)
        for row in self._rows:
            yield {field: row.get(field) for field in wanted}

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        raise AssertionError("projected path should avoid get_internal()")

    def get_inlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used directly
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return len(self._rows)

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def list_tabs(self) -> list[str]:
        return ["internal_all.csv"]

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        return iter(())

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        return iter(())

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        return iter(())


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


def _header_blob(headers: dict[str, list[str]]) -> bytes:
    payload = {
        "mHeaders": [
            {"mName": name, "mValue": values}
            for name, values in headers.items()
        ]
    }
    return json.dumps(payload).encode("utf-8")


class ChainDuckBackend(CrawlBackend):
    def __init__(self) -> None:
        addresses = [
            "https://example.com/source",
            "https://example.com/r1",
            "https://example.com/r2",
            "https://example.com/r3",
            "https://example.com/c1",
            "https://example.com/c2",
            "https://example.com/c3",
            "https://example.com/m1",
            "https://example.com/m2",
            "https://example.com/m3",
            "https://example.com/l1",
            "https://example.com/l2",
        ]
        self._tabs = {
            "internal_all.csv": [
                {
                    "Address": address,
                    "Status Code": 200,
                    "Indexability": "Indexable",
                    "Indexability Status": "Indexable",
                    "Title 1": address.rsplit("/", 1)[-1],
                }
                for address in addresses
            ]
        }
        status_overrides = {
            "https://example.com/r1": (301, "Moved Permanently", _header_blob({"location": ["/r2"]})),
            "https://example.com/r2": (302, "Found", _header_blob({"location": ["/r3"]})),
            "https://example.com/m1": (301, "Moved Permanently", _header_blob({"location": ["/m2"]})),
            "https://example.com/l1": (301, "Moved Permanently", _header_blob({"location": ["/l2"]})),
            "https://example.com/l2": (301, "Moved Permanently", _header_blob({"location": ["/l1"]})),
        }
        self._raw = {
            "APP.URLS": [],
            "APP.UNIQUE_URLS": [{"ID": index + 1, "ENCODED_URL": address} for index, address in enumerate(addresses)],
            "APP.LINKS": [],
        }
        for address in addresses:
            code, msg, headers = status_overrides.get(address, (200, "OK", None))
            self._raw["APP.URLS"].append(
                {
                    "ENCODED_URL": address,
                    "RESPONSE_CODE": code,
                    "RESPONSE_MSG": msg,
                    "CONTENT_TYPE": "text/html",
                    "NUM_METAREFRESH": 0,
                    "META_FULL_URL_1": None,
                    "META_FULL_URL_2": None,
                    "HTTP_RESPONSE_HEADER_COLLECTION": headers,
                }
            )

        id_by_url = {row["ENCODED_URL"]: row["ID"] for row in self._raw["APP.UNIQUE_URLS"]}
        def add_link(src: str, dst: str, *, link_type: int = 1, text: str = "Link", pos: int = 1) -> None:
            self._raw["APP.LINKS"].append(
                {
                    "SRC_ID": id_by_url[src],
                    "DST_ID": id_by_url[dst],
                    "ALT_TEXT": None,
                    "LINK_TEXT": text,
                    "HREF_LANG": None,
                    "NOFOLLOW": False,
                    "UGC": False,
                    "SPONSORED": False,
                    "TARGET": None,
                    "NOOPENER": False,
                    "NOREFERRER": False,
                    "PATH_TYPE": "html",
                    "ELEMENT_PATH": f"a.{text.lower().replace(' ', '-')}",
                    "ELEMENT_POSITION": pos,
                    "LINK_TYPE": link_type,
                    "SCOPE": 0,
                    "ORIGIN": 1,
                }
            )

        add_link("https://example.com/source", "https://example.com/r1", text="Redirect start", pos=1)
        add_link("https://example.com/source", "https://example.com/c1", text="Canonical start", pos=2)
        add_link("https://example.com/source", "https://example.com/m1", text="Mixed start", pos=3)
        add_link("https://example.com/source", "https://example.com/l1", text="Loop start", pos=4)
        add_link("https://example.com/c1", "https://example.com/c2", link_type=6, text="Canonical 1", pos=1)
        add_link("https://example.com/c2", "https://example.com/c3", link_type=6, text="Canonical 2", pos=1)
        add_link("https://example.com/m2", "https://example.com/m3", link_type=6, text="Canonical mixed", pos=1)

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

    exported = crawl.export_duckdb(str(target), source_label="fake-crawl", profile="full")
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


def test_export_and_load_portable_duckdb_cache(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-portable.duckdb"

    exported = crawl.export_duckdb(str(target), source_label="fake-crawl")
    duck = Crawl.from_duckdb(str(exported))

    assert exported.exists()
    assert duck.tabs == []
    broken_rows = duck.pages().filter(status_code=404).collect()
    assert len(broken_rows) == 1
    assert broken_rows[0]["Address"] == "https://example.com/broken"
    assert broken_rows[0]["Status Code"] == 404
    assert broken_rows[0]["Title 1"] == ""
    assert duck.pages().select("Address", "Title 1").collect() == [
        {"Address": "https://example.com/ok", "Title 1": "OK"},
        {"Address": "https://example.com/broken", "Title 1": ""},
    ]
    assert duck.links("in").select("Source", "Address", "Status Code", "Anchor").collect() == [
        {
            "Source": "https://example.com/source",
            "Address": "https://example.com/broken",
            "Status Code": 404,
            "Anchor": "Broken link",
        }
    ]
    with pytest.raises(NotImplementedError, match="Raw table not available"):
        next(duck.raw("APP.URLS"))


def test_export_duckdb_tabs_do_not_implicitly_export_raw_tables(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-tabs-only.duckdb"

    exported = crawl.export_duckdb(
        str(target),
        source_label="fake-crawl",
        tabs=("internal_all",),
        profile="full",
    )
    duck = Crawl.from_duckdb(str(exported))

    assert duck.pages().filter(status_code=404).collect() == [
        {"Address": "https://example.com/broken", "Status Code": 404, "Title 1": ""}
    ]
    with pytest.raises(NotImplementedError, match="Raw table not available"):
        next(duck.raw("APP.URLS"))


def test_export_duckdb_link_tabs_use_helper_path_without_raw_tables(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-links-only.duckdb"

    exported = crawl.export_duckdb(
        str(target),
        source_label="fake-crawl",
        tabs=("all_inlinks",),
        profile="full",
    )
    duck = Crawl.from_duckdb(str(exported))

    rows = duck.tab("all_inlinks").collect()
    assert rows == [
        {
            "Type": None,
            "Source": "https://example.com/source",
            "Address": "https://example.com/broken",
            "Destination": "https://example.com/broken",
            "Alt Text": None,
            "Anchor": "Broken link",
            "Status Code": 404,
            "Status": None,
            "Follow": None,
            "Target": None,
            "Rel": None,
            "Path Type": None,
            "Link Path": None,
            "Link Position": None,
            "hreflang": None,
            "Link Type": None,
            "Scope": None,
            "Origin": None,
            "NoFollow": None,
            "UGC": None,
            "Sponsored": None,
            "Noopener": None,
            "Noreferrer": None,
        }
    ]
    with pytest.raises(NotImplementedError, match="Raw table not available"):
        next(duck.raw("APP.URLS"))


def test_export_duckdb_respects_explicit_empty_raw_table_list(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-tabs-only.duckdb"

    exported = crawl.export_duckdb(
        str(target),
        source_label="fake-crawl",
        tables=(),
        tabs=("internal_all",),
    )
    duck = Crawl.from_duckdb(str(exported))

    assert duck.pages().filter(status_code=404).collect() == [
        {"Address": "https://example.com/broken", "Status Code": 404, "Title 1": ""}
    ]
    with pytest.raises(NotImplementedError, match="Raw table not available"):
        next(duck.raw("APP.URLS"))


def test_export_duckdb_can_materialize_response_codes_all_without_raw_exports(tmp_path: Path) -> None:
    target = tmp_path / "response-codes-only.duckdb"

    export_duckdb_from_backend(
        ProjectedOnlyResponseCodesBackend(),
        target,
        tables=(),
        tabs=("response_codes_all",),
        source_label="projected-response-codes",
    )
    duck = Crawl.from_duckdb(str(target))

    assert duck.tab("response_codes_all").collect() == [
        {
            "Address": "https://example.com/ok",
            "Content Type": "text/html",
            "Status Code": 200,
            "Status": "OK",
            "Indexability": "Indexable",
            "Indexability Status": None,
            "Inlinks": 4,
            "Response Time": 123,
            "Redirect URL": None,
            "Redirect Type": None,
        },
        {
            "Address": "https://example.com/redirect",
            "Content Type": "text/html",
            "Status Code": 301,
            "Status": "Moved Permanently",
            "Indexability": "Indexable",
            "Indexability Status": None,
            "Inlinks": 2,
            "Response Time": 45,
            "Redirect URL": "https://example.com/final",
            "Redirect Type": "HTTP Redirect",
        },
    ]
    assert resolve_relation_name(duck._backend.conn, "raw", "APP.URLS") is None


def test_single_crawl_duckdb_defaults_to_empty_namespace(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "single.duckdb"

    crawl.export_duckdb(str(target), source_label="single", tables=(), tabs=("internal_all",))

    assert Crawl.duckdb_namespaces(str(target)) == [""]
    duck = Crawl.from_duckdb(str(target))
    assert duck.pages().first() == {
        "Address": "https://example.com/ok",
        "Status Code": 200,
        "Title 1": "OK",
    }


def test_duckdb_can_store_multiple_crawls_by_namespace(tmp_path: Path) -> None:
    first = Crawl(
        FakeDuckCompareBackend(
            [{"Address": "https://example.com/a", "Status Code": 200, "Title 1": "A"}]
        )
    )
    second = Crawl(
        FakeDuckCompareBackend(
            [{"Address": "https://example.com/b", "Status Code": 404, "Title 1": "B"}]
        )
    )
    target = tmp_path / "multi.duckdb"

    first.export_duckdb(str(target), tables=(), tabs=("internal_all",), namespace="client-a")
    second.export_duckdb(str(target), tables=(), tabs=("internal_all",), namespace="client-b")

    assert Crawl.duckdb_namespaces(str(target)) == ["client-a", "client-b"]

    client_a = Crawl.from_duckdb(str(target), namespace="client-a")
    client_b = Crawl.from_duckdb(str(target), namespace="client-b")

    assert client_a.pages().collect() == [
        {"Address": "https://example.com/a", "Status Code": 200, "Title 1": "A"}
    ]
    assert client_b.pages().collect() == [
        {"Address": "https://example.com/b", "Status Code": 404, "Title 1": "B"}
    ]


def test_from_duckdb_requires_namespace_when_file_contains_multiple_crawls(tmp_path: Path) -> None:
    first = Crawl(
        FakeDuckCompareBackend(
            [{"Address": "https://example.com/a", "Status Code": 200, "Title 1": "A"}]
        )
    )
    second = Crawl(
        FakeDuckCompareBackend(
            [{"Address": "https://example.com/b", "Status Code": 404, "Title 1": "B"}]
        )
    )
    target = tmp_path / "multi.duckdb"

    first.export_duckdb(str(target), tables=(), tabs=("internal_all",), namespace="client-a")
    second.export_duckdb(str(target), tables=(), tabs=("internal_all",), namespace="client-b")

    with pytest.raises(ValueError, match="multiple crawl namespaces"):
        Crawl.from_duckdb(str(target))


def test_duckdb_backend_can_lazy_materialize_internal_tab_from_empty_cache(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-empty.duckdb"

    crawl.export_duckdb(
        str(target),
        source_label="fake-crawl",
        tables=(),
        tabs=(),
    )
    duck = Crawl.from_duckdb(str(target))
    duck._backend.configure_lazy_source(  # type: ignore[attr-defined]
        FakeDuckExportBackend(),
        source_label="fake-crawl",
        available_tabs=("internal_all.csv", "response_codes_internal_client_error_(4xx).csv"),
    )

    assert duck._backend._internal_relation is None  # type: ignore[attr-defined]
    assert duck.pages().count() == 2
    assert duck.tab("internal_all").first() == {
        "Address": "https://example.com/ok",
        "Status Code": 200,
        "Title 1": "OK",
    }
    assert duck.tab_columns("internal_all") == ["Address", "Status Code", "Title 1"]
    assert duck._backend._internal_relation is None  # type: ignore[attr-defined]


def test_duckdb_projected_page_view_uses_common_helper_without_internal_all(tmp_path: Path) -> None:
    source_backend = FakeDuckCompareBackend(
        [
            {
                "Address": "https://example.com/home",
                "Status Code": 200,
                "Title 1": "Home",
                "Meta Description 1": "Desc",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
                "Meta Robots 1": None,
                "X-Robots-Tag 1": None,
                "Canonical Link Element 1": None,
            }
        ]
    )
    target = tmp_path / "crawl-projected.duckdb"

    Crawl(source_backend).export_duckdb(str(target), source_label="projected", tables=(), tabs=())
    duck = Crawl.from_duckdb(str(target))
    duck._backend.configure_lazy_source(  # type: ignore[attr-defined]
        source_backend,
        source_label="projected",
        available_tabs=("internal_all.csv",),
    )

    rows = duck.pages().select("Address", "Title 1", "Meta Description 1").collect()

    assert rows == [
        {
            "Address": "https://example.com/home",
            "Title 1": "Home",
            "Meta Description 1": "Desc",
        }
    ]
    assert duck._backend._internal_relation is None  # type: ignore[attr-defined]


def test_duckdb_projected_page_view_prefers_projected_source_path(tmp_path: Path) -> None:
    source_backend = ProjectedOnlyInternalBackend()
    target = tmp_path / "crawl-projected-only.duckdb"

    Crawl(source_backend).export_duckdb(str(target), source_label="projected-only", tables=(), tabs=())
    duck = Crawl.from_duckdb(str(target))
    duck._backend.configure_lazy_source(  # type: ignore[attr-defined]
        source_backend,
        source_label="projected-only",
        available_tabs=("internal_all.csv",),
    )

    rows = duck.pages().select("Address", "Title 1", "Meta Description 1").collect()

    assert rows == [
        {
            "Address": "https://example.com/home",
            "Title 1": "Home",
            "Meta Description 1": "Desc",
        }
    ]
    assert duck._backend._internal_relation is None  # type: ignore[attr-defined]
    assert not _relation_exists(duck._backend.conn, _helper_relation_name("internal_common"))  # type: ignore[attr-defined]


def test_duckdb_backend_lazy_materializes_raw_tables_from_source(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-lazy-raw.duckdb"

    crawl.export_duckdb(
        str(target),
        source_label="fake-crawl",
        tables=(),
        tabs=("internal_all",),
    )
    duck = Crawl.from_duckdb(str(target))
    duck._backend.configure_lazy_source(  # type: ignore[attr-defined]
        FakeDuckExportBackend(),
        source_label="fake-crawl",
        available_tabs=("internal_all.csv", "response_codes_internal_client_error_(4xx).csv"),
    )

    assert next(duck.raw("APP.URLS"))["ENCODED_URL"] == "https://example.com/ok"
    assert (
        duck.query("APP", "URLS")
        .select("ENCODED_URL", "RESPONSE_CODE")
        .where("RESPONSE_CODE >= ?", 400)
        .collect()
        == [{"ENCODED_URL": "https://example.com/broken", "RESPONSE_CODE": 404}]
    )


def test_ensure_duckdb_cache_reuses_existing_db_while_read_only_connection_is_open(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-reuse.duckdb"

    crawl.export_duckdb(str(target), source_label="fake-crawl", tables=(), tabs=())
    duck = Crawl.from_duckdb(str(target))

    reused = ensure_duckdb_cache(
        target,
        source_label="fake-crawl",
        source_fingerprint=None,
        if_exists="auto",
    )

    assert reused == target
    assert duck.tabs == []


def test_duckdb_backend_lazy_materializes_tabs_from_source(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl-lazy-tab.duckdb"

    crawl.export_duckdb(
        str(target),
        source_label="fake-crawl",
        tables=(),
        tabs=("internal_all",),
    )
    duck = Crawl.from_duckdb(str(target))
    duck._backend.configure_lazy_source(  # type: ignore[attr-defined]
        FakeDuckExportBackend(),
        source_label="fake-crawl",
        available_tabs=("internal_all.csv", "response_codes_internal_client_error_(4xx).csv"),
    )

    assert "response_codes_internal_client_error_(4xx).csv" in duck.tabs
    assert duck.tab("response_codes_internal_client_error_(4xx)").collect() == [
        {"Address": "https://example.com/broken", "Status Code": 404}
    ]


def test_duckdb_projected_link_view_uses_links_core_without_link_tabs(tmp_path: Path) -> None:
    crawl = Crawl(MinimalDuckReportBackend())
    target = tmp_path / "crawl-links-projected.duckdb"

    crawl.export_duckdb(
        str(target),
        source_label="minimal-links",
        tables=(),
        tabs=("internal_all",),
    )
    duck = Crawl.from_duckdb(str(target))
    duck._backend.configure_lazy_source(  # type: ignore[attr-defined]
        MinimalDuckReportBackend(),
        source_label="minimal-links",
        available_tabs=("internal_all.csv", "all_inlinks.csv", "all_outlinks.csv"),
    )

    rows = duck.links("in").select("Source", "Address", "Status Code", "Anchor").filter(status_code=404).collect()

    assert rows == [
        {
            "Source": "https://example.com/nav",
            "Address": "https://example.com/broken-target",
            "Status Code": 404,
            "Anchor": "Broken",
        },
        {
            "Source": "https://example.com/nav",
            "Address": "https://example.com/broken-page",
            "Status Code": 404,
            "Anchor": "Broken internal",
        },
    ]
    assert resolve_relation_name(duck._backend.conn, "tab", "all_inlinks") is None  # type: ignore[attr-defined]
    assert resolve_relation_name(duck._backend.conn, "tab", "all_outlinks") is None  # type: ignore[attr-defined]
    assert not _relation_exists(duck._backend.conn, _helper_relation_name("links_core"))  # type: ignore[attr-defined]


def test_duckdb_backend_supports_links_and_chain_reports(tmp_path: Path) -> None:
    crawl = Crawl(FakeDuckExportBackend())
    target = tmp_path / "crawl.duckdb"
    crawl.export_duckdb(str(target), source_label="fake-crawl", profile="full")

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

    crawl.export_duckdb(str(target), source_label="minimal")
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

    assert sorted(broken, key=lambda row: row["Address"]) == [
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
    assert sorted(
        ((link.source, link.destination, link.anchor_text) for link in outlinks),
        key=lambda row: row[1],
    ) == [
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
        "tabs": 0,
        "broken_pages": 1,
        "broken_inlinks": 2,
        "nofollow_inlinks": 1,
        "orphan_pages": 2,
        "non_indexable_pages": 1,
        "redirect_chains": None,
        "security_issues": None,
        "canonical_issues": None,
        "hreflang_issues": None,
        "redirect_issues": None,
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


def test_duckdb_chain_helpers_work_without_materialized_chain_tabs(tmp_path: Path) -> None:
    source_backend = ChainDuckBackend()
    crawl = Crawl(source_backend)
    target = tmp_path / "crawl-chains.duckdb"

    crawl.export_duckdb(str(target), source_label="chains", tables=(), tabs=("internal_all",))
    duck = Crawl.from_duckdb(str(target))
    backend = duck._backend
    backend.configure_lazy_source(
        source_backend=source_backend,
        source_label="chains",
        available_tabs=("internal_all.csv",),
    )

    redirect_rows = list(duck.redirect_chains(min_hops=2))
    canonical_rows = list(duck.canonical_chains(min_hops=2))
    mixed_rows = list(duck.redirect_and_canonical_chains(min_hops=2))
    loop_rows = list(duck.redirect_chains(loop=True))

    redirect_by_address = {row["Address"]: row for row in redirect_rows}
    canonical_by_address = {row["Address"]: row for row in canonical_rows}
    mixed_by_address = {row["Address"]: row for row in mixed_rows}
    loop_addresses = {row["Address"] for row in loop_rows}

    assert {"https://example.com/l1", "https://example.com/l2", "https://example.com/r1"} <= set(
        redirect_by_address
    )
    assert redirect_by_address["https://example.com/r1"]["Final Address"] == "https://example.com/r3"
    assert redirect_by_address["https://example.com/r1"]["Number of Redirects"] == 2
    assert redirect_by_address["https://example.com/r1"]["Redirect Type 1"] == "HTTP Redirect"
    assert redirect_by_address["https://example.com/r1"]["Redirect URL 1"] == "https://example.com/r2"

    assert list(canonical_by_address) == ["https://example.com/c1"]
    assert canonical_by_address["https://example.com/c1"]["Number of Canonicals"] == 2
    assert canonical_by_address["https://example.com/c1"]["Final Address"] == "https://example.com/c3"
    assert canonical_by_address["https://example.com/c1"]["Chain Type"] == "Canonical"

    assert "https://example.com/m1" in mixed_by_address
    assert mixed_by_address["https://example.com/m1"]["Number of Redirects/Canonicals"] == 2
    assert mixed_by_address["https://example.com/m1"]["Chain Type"] == "Redirect & Canonical"
    assert mixed_by_address["https://example.com/m1"]["Final Address"] == "https://example.com/m3"

    assert {"https://example.com/l1", "https://example.com/l2"} <= loop_addresses
    assert all(row["Loop"] is True for row in loop_rows)
    assert resolve_relation_name(backend.conn, "raw", "APP.URLS") is None
    assert _relation_exists(backend.conn, _helper_relation_name("redirect_edges", namespace=backend.namespace))
    assert _relation_exists(backend.conn, _helper_relation_name("canonical_edges", namespace=backend.namespace))


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


def test_duckdb_compare_works_on_lean_caches_without_internal_all(tmp_path: Path) -> None:
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
        }
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
        }
    ]

    old_duckdb = tmp_path / "old-lean.duckdb"
    new_duckdb = tmp_path / "new-lean.duckdb"
    old_source = ProjectedOnlyCompareBackend(old_rows)
    new_source = ProjectedOnlyCompareBackend(new_rows)

    Crawl(old_source).export_duckdb(str(old_duckdb), tables=(), tabs=())
    Crawl(new_source).export_duckdb(str(new_duckdb), tables=(), tabs=())

    old_crawl = Crawl.from_duckdb(str(old_duckdb))
    new_crawl = Crawl.from_duckdb(str(new_duckdb))
    old_crawl._backend.configure_lazy_source(  # type: ignore[attr-defined]
        old_source,
        source_label="old-lean",
        available_tabs=("internal_all.csv",),
    )
    new_crawl._backend.configure_lazy_source(  # type: ignore[attr-defined]
        new_source,
        source_label="new-lean",
        available_tabs=("internal_all.csv",),
    )

    diff = new_crawl.compare(old_crawl)

    assert [(change.url, change.old_status, change.new_status) for change in diff.status_changes] == [
        ("https://example.com/home", 200, 301)
    ]
    assert [(change.url, change.old_title, change.new_title) for change in diff.title_changes] == [
        ("https://example.com/home", "Home", "Homepage")
    ]
    assert new_crawl._backend._internal_relation is None  # type: ignore[attr-defined]
    assert old_crawl._backend._internal_relation is None  # type: ignore[attr-defined]


def test_bulk_load_syscs_csvs_to_duckdb_preserves_column_names_without_headers(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "APP_URLS.csv"
    csv_path.write_text(
        "1,https://example.com/ok,200\n2,https://example.com/broken,404\n",
        encoding="utf-8",
    )
    target = tmp_path / "syscs-no-header.duckdb"
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(target))
    try:
        objects = _bulk_load_syscs_csvs_to_duckdb(
            conn,
            {
                "APP.URLS": {
                    "path": csv_path,
                    "columns": ["ID", "ENCODED_URL", "RESPONSE_CODE"],
                    "row_count": 2,
                }
            },
        )
        assert objects == [("APP.URLS", "raw", "app.urls")]
        relation = objects[0][2]
        assert relation == "app.urls"
        rows = list(iter_relation_rows(conn, relation))
        assert rows == [
            {"ID": 1, "ENCODED_URL": "https://example.com/ok", "RESPONSE_CODE": 200},
            {"ID": 2, "ENCODED_URL": "https://example.com/broken", "RESPONSE_CODE": 404},
        ]
    finally:
        conn.close()


def test_bulk_load_syscs_csvs_to_duckdb_rejects_row_count_mismatch(tmp_path: Path) -> None:
    csv_path = tmp_path / "APP_URLS.csv"
    csv_path.write_text(
        "1,https://example.com/ok,200\n2,https://example.com/broken,404\n",
        encoding="utf-8",
    )
    target = tmp_path / "syscs-mismatch.duckdb"
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(target))
    try:
        objects = _bulk_load_syscs_csvs_to_duckdb(
            conn,
            {
                "APP.URLS": {
                    "path": csv_path,
                    "columns": ["ID", "ENCODED_URL", "RESPONSE_CODE"],
                    "row_count": 3,
                }
            },
        )
        assert objects == []
        assert not _relation_exists(conn, "app.urls")
    finally:
        conn.close()
