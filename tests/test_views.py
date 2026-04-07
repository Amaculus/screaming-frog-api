from __future__ import annotations

import csv
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterator, Optional, Sequence

import pytest

from screamingfrog import Crawl
from screamingfrog.backends.base import CrawlBackend
from screamingfrog.models import InternalPage
from screamingfrog.models.diff import CrawlDiff


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_pages_and_links_views_from_csv(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [
            {"Address": "https://example.com/blog/a", "Status Code": "200"},
            {"Address": "https://example.com/shop/b", "Status Code": "404"},
        ],
    )
    _write_csv(
        tmp_path / "all_inlinks.csv",
        [
            {
                "Address": "https://example.com/blog/a",
                "Source": "https://example.com/source",
                "Status Code": "200",
            }
        ],
    )
    _write_csv(
        tmp_path / "all_outlinks.csv",
        [
            {
                "Source": "https://example.com/blog/a",
                "Destination": "https://example.com/destination",
                "Status Code": "200",
            }
        ],
    )

    crawl = Crawl.load(str(tmp_path))

    pages = crawl.pages().filter(status_code=404).collect()
    inlinks = crawl.links("in").collect()
    outlinks = crawl.links("out").collect()

    assert pages == [{"Address": "https://example.com/shop/b", "Status Code": "404"}]
    assert inlinks[0]["Source"] == "https://example.com/source"
    assert outlinks[0]["Destination"] == "https://example.com/destination"


def test_page_view_select_projects_requested_fields(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [
            {
                "Address": "https://example.com/blog/a",
                "Status Code": "200",
                "Title 1": "Blog A",
                "Meta Description 1": "Desc A",
            },
            {
                "Address": "https://example.com/shop/b",
                "Status Code": "404",
                "Title 1": "",
                "Meta Description 1": "",
            },
        ],
    )

    crawl = Crawl.load(str(tmp_path))

    rows = crawl.pages().select("Address", "Title 1").filter(status_code=404).collect()

    assert rows == [{"Address": "https://example.com/shop/b", "Title 1": ""}]


def test_link_view_select_projects_requested_fields(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [{"Address": "https://example.com/", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "all_inlinks.csv",
        [
            {
                "Address": "https://example.com/blog/a",
                "Source": "https://example.com/source",
                "Anchor": "Blog A",
                "Status Code": "200",
            },
            {
                "Address": "https://example.com/shop/b",
                "Source": "https://example.com/source",
                "Anchor": "Shop B",
                "Status Code": "404",
            },
        ],
    )

    crawl = Crawl.load(str(tmp_path))

    rows = crawl.links("in").select("Source", "Address").filter(status_code=404).collect()

    assert rows == [
        {
            "Source": "https://example.com/source",
            "Address": "https://example.com/shop/b",
        }
    ]


def test_section_scopes_pages_and_links_by_path_prefix(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [
            {"Address": "https://example.com/blog/a", "Status Code": "200"},
            {"Address": "https://example.com/shop/b", "Status Code": "200"},
        ],
    )
    _write_csv(
        tmp_path / "all_inlinks.csv",
        [
            {
                "Address": "https://example.com/blog/a",
                "Source": "https://example.com/source",
            },
            {
                "Address": "https://example.com/shop/b",
                "Source": "https://example.com/source",
            },
        ],
    )
    _write_csv(
        tmp_path / "all_outlinks.csv",
        [
            {
                "Source": "https://example.com/blog/a",
                "Destination": "https://example.com/destination",
            },
            {
                "Source": "https://example.com/shop/b",
                "Destination": "https://example.com/destination",
            },
        ],
    )

    crawl = Crawl.load(str(tmp_path))
    blog = crawl.section("/blog")

    assert [row["Address"] for row in blog.pages().collect()] == ["https://example.com/blog/a"]
    assert [row["Address"] for row in blog.links("in").collect()] == ["https://example.com/blog/a"]
    assert [row["Source"] for row in blog.links("out").collect()] == ["https://example.com/blog/a"]


def test_search_views_and_section_tab_scope(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [
            {
                "Address": "https://example.com/blog/python-guide",
                "Title 1": "Python Migration Guide",
                "Status Code": "200",
            },
            {
                "Address": "https://example.com/shop/widget",
                "Title 1": "Widget",
                "Status Code": "200",
            },
        ],
    )
    _write_csv(
        tmp_path / "all_inlinks.csv",
        [
            {
                "Address": "https://example.com/blog/python-guide",
                "Source": "https://example.com/docs/python",
                "Anchor": "Python guide",
            },
            {
                "Address": "https://example.com/shop/widget",
                "Source": "https://example.com/docs/widget",
                "Anchor": "Widget",
            },
        ],
    )

    crawl = Crawl.load(str(tmp_path))

    assert crawl.search("python", fields=["Address"]).collect() == [
        {
            "Address": "https://example.com/blog/python-guide",
            "Title 1": "Python Migration Guide",
            "Status Code": "200",
        }
    ]
    assert crawl.links("in").search("guide", fields=["Anchor"]).collect() == [
        {
            "Address": "https://example.com/blog/python-guide",
            "Source": "https://example.com/docs/python",
            "Anchor": "Python guide",
        }
    ]
    assert crawl.section("/blog").tab("all_inlinks").collect() == [
        {
            "Address": "https://example.com/blog/python-guide",
            "Source": "https://example.com/docs/python",
            "Anchor": "Python guide",
        }
    ]


def test_links_rejects_invalid_direction(tmp_path: Path) -> None:
    _write_csv(tmp_path / "internal_all.csv", [{"Address": "https://example.com/", "Status Code": "200"}])
    crawl = Crawl.load(str(tmp_path))

    try:
        crawl.links("sideways")
    except ValueError as exc:
        assert "direction" in str(exc)
    else:
        raise AssertionError("Expected crawl.links() to reject an invalid direction")


class FakeBackend(CrawlBackend):
    def __init__(self) -> None:
        self.closed = False

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        yield InternalPage.from_data({"Address": "https://example.com/internal", "Status Code": 200})

    def get_inlinks(self, url: str):  # pragma: no cover - not used here
        return iter(())

    def get_outlinks(self, url: str):  # pragma: no cover - not used here
        return iter(())

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return 1

    def aggregate(self, table: str, column: str, func: str) -> Any:  # pragma: no cover
        return None

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        yield {"Address": "https://example.com/tab", "Status Code": 201}

    def raw(self, table: str) -> Iterator[dict[str, Any]]:  # pragma: no cover
        yield {"value": 1}

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        yield {"Address": "https://example.com/query", "Status Code": 202}

    def close(self) -> None:
        self.closed = True


def test_view_dataframe_exports(monkeypatch) -> None:
    fake_pandas = SimpleNamespace(DataFrame=lambda rows: {"engine": "pandas", "rows": rows})
    fake_polars = SimpleNamespace(DataFrame=lambda rows: {"engine": "polars", "rows": rows})
    monkeypatch.setitem(sys.modules, "pandas", fake_pandas)
    monkeypatch.setitem(sys.modules, "polars", fake_polars)

    crawl = Crawl(FakeBackend())

    internal_df = crawl.internal.to_pandas()
    tab_df = crawl.tab("internal_all").to_polars()
    query_df = crawl.query("APP", "URLS").to_pandas()

    assert internal_df["engine"] == "pandas"
    assert internal_df["rows"][0]["Address"] == "https://example.com/internal"
    assert tab_df["engine"] == "polars"
    assert tab_df["rows"][0]["Address"] == "https://example.com/tab"
    assert query_df["rows"][0]["Address"] == "https://example.com/query"


def test_diff_helpers_and_dataframe_export(monkeypatch) -> None:
    fake_pandas = SimpleNamespace(DataFrame=lambda rows: {"engine": "pandas", "rows": rows})
    monkeypatch.setitem(sys.modules, "pandas", fake_pandas)

    diff = CrawlDiff(
        added_pages=["https://example.com/new"],
        removed_pages=["https://example.com/old"],
        status_changes=[],
        title_changes=[],
        redirect_changes=[],
    )

    summary = diff.summary()
    rows = diff.to_rows()
    frame = diff.to_pandas()

    assert summary["added_pages"] == 1
    assert summary["removed_pages"] == 1
    assert summary["total_changes"] == 2
    assert {"change_type": "added_page", "url": "https://example.com/new"} in rows
    assert frame["rows"] == rows


def test_internal_search_uses_page_data() -> None:
    crawl = Crawl(FakeBackend())

    rows = crawl.internal.search("internal", fields=["Address"]).collect()

    assert rows[0].address == "https://example.com/internal"


def test_csv_internal_requires_core_headers(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [{"Address": "https://example.com/missing-status"}],
    )

    crawl = Crawl.load(str(tmp_path))

    with pytest.raises(ValueError, match="missing required columns"):
        list(crawl.internal)


def test_crawl_context_manager_closes_backend() -> None:
    backend = FakeBackend()

    with Crawl(backend) as crawl:
        assert crawl.internal.first().address == "https://example.com/internal"

    assert backend.closed is True
