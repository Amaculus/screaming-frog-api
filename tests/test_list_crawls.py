from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from screamingfrog import CrawlInfo, list_crawls


def _make_crawl_dir(root: Path, db_id: str, url: str, urls_crawled: int, ts: int) -> None:
    """Create a minimal mock crawl directory with metadata files."""
    d = root / db_id
    d.mkdir()
    escaped_url = url.replace(":", "\\:")
    (d / "DbSeoSpiderFileKey").write_text(
        f"#Mon Jan 01 00:00:00 UTC 2024\n"
        f"url={escaped_url}\n"
        f"version=3\n"
    )
    (d / "DbSeoSpiderFileKeyDynamic").write_text(
        f"#Mon Jan 01 00:00:00 UTC 2024\n"
        f"modifiedTime={ts}\n"
        f"percentComplete=100.0\n"
        f"urlsCrawled={urls_crawled}\n"
    )


def test_list_crawls_returns_crawl_info(tmp_path):
    _make_crawl_dir(tmp_path, "aaa-111", "https://example.com/", 500, 1700000000)
    _make_crawl_dir(tmp_path, "bbb-222", "https://other.com/", 1000, 1700100000)

    results = list_crawls(project_root=tmp_path)

    assert len(results) == 2
    assert all(isinstance(r, CrawlInfo) for r in results)


def test_list_crawls_sorted_most_recent_first(tmp_path):
    _make_crawl_dir(tmp_path, "old", "https://old.com/", 10, 1000000)
    _make_crawl_dir(tmp_path, "new", "https://new.com/", 20, 9000000)

    results = list_crawls(project_root=tmp_path)

    assert results[0].db_id == "new"
    assert results[1].db_id == "old"


def test_list_crawls_parses_metadata(tmp_path):
    _make_crawl_dir(tmp_path, "abc-123", "https://example.com/", 42, 1700000000)

    info = list_crawls(project_root=tmp_path)[0]

    assert info.db_id == "abc-123"
    assert info.url == "https://example.com/"
    assert info.urls_crawled == 42
    assert info.percent_complete == 100.0
    assert info.modified == datetime.fromtimestamp(1700000000, tz=timezone.utc)
    assert info.path == tmp_path / "abc-123"


def test_list_crawls_empty_directory(tmp_path):
    assert list_crawls(project_root=tmp_path) == []


def test_list_crawls_skips_dirs_without_key(tmp_path):
    (tmp_path / "not-a-crawl").mkdir()
    _make_crawl_dir(tmp_path, "real", "https://real.com/", 5, 1000)

    results = list_crawls(project_root=tmp_path)

    assert len(results) == 1
    assert results[0].db_id == "real"


def test_crawl_info_str(tmp_path):
    _make_crawl_dir(tmp_path, "abc-123", "https://example.com/", 1500, 1700000000)

    info = list_crawls(project_root=tmp_path)[0]
    s = str(info)

    assert "https://example.com/" in s
    assert "1,500" in s
    assert "abc-123" in s
