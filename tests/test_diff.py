from __future__ import annotations

import csv
from pathlib import Path

from screamingfrog import Crawl


def _write_internal(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "Address",
                "Status Code",
                "Title 1",
                "Redirect URL",
                "Canonical Link Element 1",
                "Meta Description 1",
                "Meta Keywords 1",
                "H1-1",
                "H2-1",
                "H3-1",
                "Word Count",
                "Indexability",
                "Indexability Status",
                "Meta Robots 1",
                "X-Robots-Tag 1",
                "Meta Refresh 1",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_crawl_compare_csv(tmp_path: Path) -> None:
    old_dir = tmp_path / "old"
    new_dir = tmp_path / "new"
    old_dir.mkdir()
    new_dir.mkdir()

    _write_internal(
        old_dir / "internal_all.csv",
        [
            {
                "Address": "https://example.com/a",
                "Status Code": "200",
                "Title 1": "Old Title",
                "Redirect URL": "",
                "Canonical Link Element 1": "https://example.com/a",
                "Meta Description 1": "Old description",
                "Meta Keywords 1": "alpha, beta",
                "H1-1": "Old H1",
                "H2-1": "Old H2",
                "H3-1": "Old H3",
                "Word Count": "100",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
                "Meta Robots 1": "index,follow",
                "X-Robots-Tag 1": "",
                "Meta Refresh 1": "",
            },
            {
                "Address": "https://example.com/b",
                "Status Code": "404",
                "Title 1": "B",
                "Redirect URL": "",
                "Canonical Link Element 1": "",
                "Meta Description 1": "",
                "Meta Keywords 1": "",
                "H1-1": "",
                "H2-1": "",
                "H3-1": "",
                "Word Count": "",
                "Indexability": "",
                "Indexability Status": "",
                "Meta Robots 1": "",
                "X-Robots-Tag 1": "",
                "Meta Refresh 1": "",
            },
            {
                "Address": "https://example.com/d",
                "Status Code": "200",
                "Title 1": "D",
                "Redirect URL": "",
                "Canonical Link Element 1": "https://example.com/d",
                "Meta Description 1": "",
                "Meta Keywords 1": "",
                "H1-1": "",
                "H2-1": "",
                "H3-1": "",
                "Word Count": "",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
                "Meta Robots 1": "",
                "X-Robots-Tag 1": "",
                "Meta Refresh 1": "",
            },
        ],
    )

    _write_internal(
        new_dir / "internal_all.csv",
        [
            {
                "Address": "https://example.com/a",
                "Status Code": "301",
                "Title 1": "New Title",
                "Redirect URL": "https://example.com/a2",
                "Canonical Link Element 1": "https://example.com/d",
                "Meta Description 1": "New description",
                "Meta Keywords 1": "alpha, gamma",
                "H1-1": "New H1",
                "H2-1": "New H2",
                "H3-1": "New H3",
                "Word Count": "120",
                "Indexability": "Non-Indexable",
                "Indexability Status": "Noindex",
                "Meta Robots 1": "noindex,nofollow",
                "X-Robots-Tag 1": "noindex",
                "Meta Refresh 1": "https://example.com/refresh",
            },
            {
                "Address": "https://example.com/c",
                "Status Code": "200",
                "Title 1": "C",
                "Redirect URL": "",
                "Canonical Link Element 1": "",
                "Meta Description 1": "",
                "Meta Keywords 1": "",
                "H1-1": "",
                "H2-1": "",
                "H3-1": "",
                "Word Count": "",
                "Indexability": "",
                "Indexability Status": "",
                "Meta Robots 1": "",
                "X-Robots-Tag 1": "",
                "Meta Refresh 1": "",
            },
            {
                "Address": "https://example.com/d",
                "Status Code": "200",
                "Title 1": "D",
                "Redirect URL": "",
                "Canonical Link Element 1": "https://example.com/d",
                "Meta Description 1": "",
                "Meta Keywords 1": "",
                "H1-1": "",
                "H2-1": "",
                "H3-1": "",
                "Word Count": "",
                "Indexability": "Non-Indexable",
                "Indexability Status": "Noindex",
                "Meta Robots 1": "noindex",
                "X-Robots-Tag 1": "",
                "Meta Refresh 1": "",
            },
        ],
    )

    old_crawl = Crawl.load(str(old_dir))
    new_crawl = Crawl.load(str(new_dir))

    diff = new_crawl.compare(old_crawl)

    assert diff.added_pages == ["https://example.com/c"]
    assert diff.removed_pages == ["https://example.com/b"]
    assert len(diff.status_changes) == 1
    assert diff.status_changes[0].url == "https://example.com/a"
    assert diff.status_changes[0].old_status == 200
    assert diff.status_changes[0].new_status == 301
    assert len(diff.title_changes) == 1
    assert diff.title_changes[0].old_title == "Old Title"
    assert diff.title_changes[0].new_title == "New Title"
    assert len(diff.redirect_changes) == 1
    assert diff.redirect_changes[0].old_target is None
    assert diff.redirect_changes[0].new_target == "https://example.com/a2"
    fields = {(c.field, c.old_value, c.new_value) for c in diff.field_changes}
    assert ("Canonical", "https://example.com/a", "https://example.com/d") in fields
    assert ("Canonical Status", "self-ref", "non-indexable") in fields
    assert ("Meta Description", "Old description", "New description") in fields
    assert ("Meta Keywords", "alpha, beta", "alpha, gamma") in fields
    assert ("H1-1", "Old H1", "New H1") in fields
    assert ("H2-1", "Old H2", "New H2") in fields
    assert ("H3-1", "Old H3", "New H3") in fields
    assert ("Word Count", "100", "120") in fields
    assert ("Indexability", "Indexable", "Non-Indexable") in fields
    assert ("Indexability Status", "Indexable", "Noindex") in fields
    assert ("Meta Robots", "index,follow", "noindex,nofollow") in fields
    assert ("X-Robots-Tag", None, "noindex") in fields
    assert ("Meta Refresh", None, "https://example.com/refresh") in fields
    assert ("Directives Summary", "follow,index", "nofollow,noindex") in fields
