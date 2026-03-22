from __future__ import annotations

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
