from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from screamingfrog import Crawl
from screamingfrog.backends.base import CrawlBackend
from screamingfrog.backends.derby_backend import _normalize_select_expression


@pytest.fixture()
def sample_export_dir(tmp_path: Path) -> Path:
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    csv_path = export_dir / "internal_all.csv"
    rows = [
        {"Address": "https://example.com/", "Status Code": "200"},
        {"Address": "https://example.com/missing", "Status Code": "404"},
        {"Address": "https://example.com/redirect", "Status Code": "301"},
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["Address", "Status Code"])
        writer.writeheader()
        writer.writerows(rows)
    return export_dir


@pytest.fixture()
def sample_db_path(tmp_path: Path) -> Path:
    db_path = tmp_path / "crawl.dbseospider"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE internal (id INTEGER PRIMARY KEY, address TEXT NOT NULL, status_code INTEGER, title TEXT, meta_description TEXT)"
    )
    conn.execute(
        "INSERT INTO internal (address, status_code, title, meta_description) VALUES (?, ?, ?, ?)",
        ("https://example.com/", 200, "Home", "Welcome"),
    )
    conn.execute(
        "INSERT INTO internal (address, status_code, title, meta_description) VALUES (?, ?, ?, ?)",
        ("https://example.com/missing", 404, "", ""),
    )
    conn.execute(
        "INSERT INTO internal (address, status_code, title, meta_description) VALUES (?, ?, ?, ?)",
        ("https://example.com/redirect", 301, "Redirect", "Redirecting"),
    )
    conn.commit()
    conn.close()
    return db_path


def test_backends_return_identical_results(sample_export_dir: Path, sample_db_path: Path) -> None:
    csv_crawl = Crawl.from_exports(str(sample_export_dir))
    db_crawl = Crawl.from_database(str(sample_db_path))

    csv_urls = [page.address for page in csv_crawl.internal.filter(status_code=404)]
    db_urls = [page.address for page in db_crawl.internal.filter(status_code=404)]

    assert csv_urls == db_urls


def test_crawl_load_auto_detect(sample_export_dir: Path, sample_db_path: Path) -> None:
    csv_crawl = Crawl.load(str(sample_export_dir))
    db_crawl = Crawl.load(str(sample_db_path))

    assert csv_crawl.internal.count() == 3
    assert db_crawl.internal.count() == 3


def test_generic_tab_access(sample_export_dir: Path) -> None:
    crawl = Crawl.load(str(sample_export_dir))
    tabs = crawl.tabs
    assert "internal_all.csv" in {t.lower() for t in tabs}

    rows = list(crawl.tab("internal_all").filter(status_code="404"))
    assert len(rows) == 1
    assert rows[0]["Address"] == "https://example.com/missing"


def test_csv_backend_reads_exported_inlinks_and_outlinks(sample_export_dir: Path) -> None:
    for name in ("all_inlinks.csv", "all_outlinks.csv"):
        with (sample_export_dir / name).open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["Source", "Destination", "Anchor"])
            writer.writeheader()
            writer.writerow(
                {
                    "Source": "https://example.com/source",
                    "Destination": "https://example.com/target",
                    "Anchor": "Target",
                }
            )

    crawl = Crawl.from_exports(str(sample_export_dir))

    assert [link.source for link in crawl.inlinks("https://example.com/target")] == [
        "https://example.com/source"
    ]
    assert [link.destination for link in crawl.outlinks("https://example.com/source")] == [
        "https://example.com/target"
    ]


def test_sqlite_backend_reads_optional_link_table(sample_db_path: Path) -> None:
    conn = sqlite3.connect(str(sample_db_path))
    conn.execute('CREATE TABLE links ("Source" TEXT, "Destination" TEXT, "Anchor Text" TEXT)')
    conn.execute(
        'INSERT INTO links VALUES (?, ?, ?)',
        ("https://example.com/source", "https://example.com/target", "Target"),
    )
    conn.commit()
    conn.close()
    crawl = Crawl.from_database(str(sample_db_path))

    assert [link.anchor_text for link in crawl.inlinks("https://example.com/target")] == [
        "Target"
    ]
    assert [link.destination for link in crawl.outlinks("https://example.com/source")] == [
        "https://example.com/target"
    ]


def test_tab_count_and_rows_use_csv_backend_fast_paths(sample_export_dir: Path) -> None:
    crawl = Crawl.load(str(sample_export_dir))

    assert crawl.tab_count("internal_all") == 3
    assert crawl.tab_count("internal_all", filters={"status_code": "404"}) == 1
    assert crawl.tab_counts(["internal_all"]) == {"internal_all": 3}
    assert crawl.tab_rows("internal_all", limit=2) == [
        {"Address": "https://example.com/", "Status Code": "200"},
        {"Address": "https://example.com/missing", "Status Code": "404"},
    ]
    assert crawl.tab("internal_all").filter(status_code="404").count() == 1
    assert crawl.tab("internal_all").filter(status_code="404").first() == {
        "Address": "https://example.com/missing",
        "Status Code": "404",
    }
    assert crawl.tab("internal_all").select("Address").filter(status_code="404").collect() == [
        {"Address": "https://example.com/missing"}
    ]


def test_sqlite_tab_support(sample_db_path: Path) -> None:
    crawl = Crawl.from_database(str(sample_db_path))
    rows = list(crawl.tab("response_codes_internal_client_error_(4xx)"))
    assert len(rows) == 1
    assert rows[0]["Address"] == "https://example.com/missing"

    missing_titles = list(crawl.tab("page_titles_missing"))
    assert len(missing_titles) == 1
    assert missing_titles[0]["Address"] == "https://example.com/missing"

    missing_meta = list(crawl.tab("meta_description_missing"))
    assert len(missing_meta) == 1
    assert missing_meta[0]["Address"] == "https://example.com/missing"


def test_tab_count_and_rows_use_sqlite_backend_fast_paths(sample_db_path: Path) -> None:
    crawl = Crawl.from_database(str(sample_db_path))

    assert crawl.tab_count("response_codes_internal_client_error_(4xx)") == 1
    assert crawl.tab_counts(["response_codes_internal_client_error_(4xx)"]) == {
        "response_codes_internal_client_error_(4xx)": 1
    }
    rows = crawl.tab_rows("response_codes_internal_client_error_(4xx)", limit=1)
    assert len(rows) == 1
    assert rows[0]["Address"] == "https://example.com/missing"
    assert rows[0]["Status Code"] == 404
    assert crawl.tab("response_codes_internal_client_error_(4xx)").count() == 1
    assert (
        crawl.tab("response_codes_internal_client_error_(4xx)").first()["Address"]
        == "https://example.com/missing"
    )
    assert crawl.tab("response_codes_internal_client_error_(4xx)").select("Address").collect() == [
        {"Address": "https://example.com/missing"}
    ]


def test_report_counts_handles_missing_optional_reports(sample_export_dir: Path) -> None:
    crawl = Crawl.load(str(sample_export_dir))

    counts = crawl.report_counts()

    assert counts["internal_urls"] == 3
    assert counts["client_error_urls"] == 0
    assert counts["security_issues"] == 0


def test_report_counts_uses_backend_batch_counts() -> None:
    class BatchCountBackend(CrawlBackend):
        def __init__(self) -> None:
            self.tab_counts_calls: list[list[str]] = []
            self.tab_count_calls: list[str] = []

        def get_internal(self, filters=None):
            return iter(())

        def get_inlinks(self, url: str):
            return iter(())

        def get_outlinks(self, url: str):
            return iter(())

        def count(self, table: str, filters=None) -> int:
            return 0

        def aggregate(self, table: str, column: str, func: str):
            return None

        def tab_counts(self, tab_names, filters=None):
            names = [str(name) for name in tab_names]
            self.tab_counts_calls.append(names)
            return {name: 1 for name in names}

        def tab_count(self, tab_name: str, filters=None) -> int:
            self.tab_count_calls.append(str(tab_name))
            return 1

    backend = BatchCountBackend()
    counts = Crawl(backend).report_counts()

    assert counts["internal_urls"] == 1
    assert counts["redirect_chains"] == 1
    assert backend.tab_counts_calls
    assert backend.tab_count_calls == []


def test_fast_tab_helpers_match_iterated_csv_tab(sample_export_dir: Path) -> None:
    crawl = Crawl.load(str(sample_export_dir))
    rows = list(crawl.tab("internal_all"))

    assert crawl.tab_count("internal_all") == len(rows)
    assert crawl.tab_rows("internal_all", limit=2) == rows[:2]
    assert crawl.tab("internal_all").select("Address", "Status Code").collect() == [
        {"Address": row.get("Address"), "Status Code": row.get("Status Code")}
        for row in rows
    ]


def test_fast_tab_helpers_match_iterated_sqlite_tab(sample_db_path: Path) -> None:
    crawl = Crawl.from_database(str(sample_db_path))
    tab_name = "response_codes_internal_client_error_(4xx)"
    rows = list(crawl.tab(tab_name))

    assert crawl.tab_count(tab_name) == len(rows)
    assert crawl.tab_rows(tab_name, limit=2) == rows[:2]
    assert crawl.tab(tab_name).select("Address", "Status Code").collect() == [
        {"Address": row.get("Address"), "Status Code": row.get("Status Code")}
        for row in rows
    ]


def test_sqlite_internal_view_matches_internal_all_projection(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.dbseospider"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE internal ("
        "id INTEGER PRIMARY KEY, address TEXT NOT NULL, status_code INTEGER, "
        "indexability TEXT, indexability_status TEXT, title TEXT)"
    )
    conn.executemany(
        "INSERT INTO internal (address, status_code, indexability, indexability_status, title) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("https://example.com/", 200, "Indexable", "Indexable", "Home"),
            ("https://example.com/noindex", 200, "Non-Indexable", "Noindex", "Noindex"),
        ],
    )
    conn.commit()
    conn.close()

    crawl = Crawl.from_database(str(db_path))
    pages = list(crawl.internal.filter(indexability="Indexable"))

    assert [page.address for page in pages] == ["https://example.com/"]
    assert pages[0].data["Indexability"] == "Indexable"
    assert pages[0].data["Indexability Status"] == "Indexable"
    assert crawl.internal.filter(indexability="Indexable").count() == 1


# ── Tests for _normalize_select_expression Derby correlated-subquery fix ──────

_DST_BROKEN = (
    "(SELECT u.PAGE_SIZE FROM APP.URLS u "
    "JOIN APP.UNIQUE_URLS d ON d.ID = APP.LINKS.DST_ID "
    "WHERE u.ENCODED_URL = d.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
)
_DST_FIXED = (
    "(SELECT u.PAGE_SIZE FROM APP.URLS u "
    "WHERE u.ENCODED_URL = ("
    "SELECT uu.ENCODED_URL FROM APP.UNIQUE_URLS uu "
    "WHERE uu.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY) FETCH FIRST 1 ROWS ONLY)"
)

_SRC_BROKEN = (
    "(SELECT u.SEGMENTS FROM APP.URLS u "
    "JOIN APP.UNIQUE_URLS s ON s.ID = APP.LINKS.SRC_ID "
    "WHERE u.ENCODED_URL = s.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
)
_SRC_FIXED = (
    "(SELECT u.SEGMENTS FROM APP.URLS u "
    "WHERE u.ENCODED_URL = ("
    "SELECT uu.ENCODED_URL FROM APP.UNIQUE_URLS uu "
    "WHERE uu.ID = APP.LINKS.SRC_ID FETCH FIRST 1 ROWS ONLY) FETCH FIRST 1 ROWS ONLY)"
)


def test_normalize_rewrites_dst_id_join_pattern() -> None:
    """Derby rejects APP.LINKS.DST_ID correlated ref inside a subquery JOIN ON clause.
    _normalize_select_expression must rewrite it to a WHERE-only equivalent."""
    result = _normalize_select_expression(_DST_BROKEN)
    assert "JOIN APP.UNIQUE_URLS" not in result
    assert "APP.LINKS.DST_ID" in result
    assert "WHERE uu.ID = APP.LINKS.DST_ID" in result
    assert result == _DST_FIXED


def test_normalize_rewrites_src_id_join_pattern() -> None:
    """Same fix for the SRC_ID variant used in Source Segments / source-side lookups."""
    result = _normalize_select_expression(_SRC_BROKEN)
    assert "JOIN APP.UNIQUE_URLS" not in result
    assert "APP.LINKS.SRC_ID" in result
    assert "WHERE uu.ID = APP.LINKS.SRC_ID" in result
    assert result == _SRC_FIXED


def test_normalize_leaves_unrelated_expressions_unchanged() -> None:
    """Expressions that don't contain the broken pattern must pass through untouched."""
    plain = "(SELECT s.ENCODED_URL FROM APP.UNIQUE_URLS s WHERE s.ID = APP.LINKS.SRC_ID FETCH FIRST 1 ROWS ONLY)"
    assert _normalize_select_expression(plain) == plain
