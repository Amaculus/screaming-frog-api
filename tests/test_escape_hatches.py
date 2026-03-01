from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from screamingfrog import Crawl


def _make_export_dir(tmp_path: Path) -> Path:
    export_dir = tmp_path / "exports"
    export_dir.mkdir()
    csv_path = export_dir / "internal_all.csv"
    rows = [
        {"Address": "https://example.com/", "Status Code": "200"},
    ]
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["Address", "Status Code"])
        writer.writeheader()
        writer.writerows(rows)
    return export_dir


def _make_sqlite(tmp_path: Path) -> Path:
    db_path = tmp_path / "crawl.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE internal (address TEXT NOT NULL, status_code INTEGER, title TEXT)"
    )
    conn.execute(
        "INSERT INTO internal (address, status_code, title) VALUES (?, ?, ?)",
        ("https://example.com/", 200, "Home"),
    )
    conn.commit()
    conn.close()
    return db_path


def test_escape_hatches_sqlite(tmp_path: Path) -> None:
    db_path = _make_sqlite(tmp_path)
    crawl = Crawl.load(str(db_path))

    rows = list(crawl.raw("internal"))
    assert rows and rows[0]["address"] == "https://example.com/"

    rows = list(
        crawl.sql("SELECT address FROM internal WHERE status_code = ?", [200])
    )
    assert rows == [{"address": "https://example.com/"}]


def test_escape_hatches_csv_not_supported(tmp_path: Path) -> None:
    export_dir = _make_export_dir(tmp_path)
    crawl = Crawl.load(str(export_dir))

    with pytest.raises(NotImplementedError):
        list(crawl.raw("internal"))

    with pytest.raises(NotImplementedError):
        list(crawl.sql("SELECT * FROM internal"))
