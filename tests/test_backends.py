from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from screamingfrog import Crawl


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
