from __future__ import annotations

import csv
from pathlib import Path

from screamingfrog import Crawl


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_audit_helpers_from_internal_csv(tmp_path: Path) -> None:
    fieldnames = [
        "Address",
        "Status Code",
        "Title 1",
        "Meta Description 1",
        "Indexability",
        "Indexability Status",
        "Canonical Link Element 1",
        "Meta Robots 1",
        "X-Robots-Tag 1",
    ]
    _write_csv(
        tmp_path / "internal_all.csv",
        fieldnames,
        [
            {
                "Address": "https://example.com/ok",
                "Status Code": "200",
                "Title 1": "Home",
                "Meta Description 1": "Desc",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
                "Canonical Link Element 1": "https://example.com/ok",
                "Meta Robots 1": "",
                "X-Robots-Tag 1": "",
            },
            {
                "Address": "https://example.com/missing",
                "Status Code": "404",
                "Title 1": "",
                "Meta Description 1": "",
                "Indexability": "Non-Indexable",
                "Indexability Status": "Noindex",
                "Canonical Link Element 1": "",
                "Meta Robots 1": "noindex",
                "X-Robots-Tag 1": "",
            },
            {
                "Address": "https://example.com/error",
                "Status Code": "500",
                "Title 1": "Server Error",
                "Meta Description 1": "Broken",
                "Indexability": "Non-Indexable",
                "Indexability Status": "Blocked",
                "Canonical Link Element 1": "",
                "Meta Robots 1": "",
                "X-Robots-Tag 1": "noindex",
            },
        ],
    )

    crawl = Crawl.load(str(tmp_path))

    broken = crawl.broken_links_report()
    title_meta = crawl.title_meta_audit()
    indexability = crawl.indexability_audit()

    assert [row["Address"] for row in broken] == [
        "https://example.com/missing",
        "https://example.com/error",
    ]
    assert broken[0]["Inlinks"] == 0
    assert {"Address": "https://example.com/missing", "Issue": "Missing Title"} in title_meta
    assert {
        "Address": "https://example.com/missing",
        "Issue": "Missing Meta Description",
    } in title_meta
    assert [row["Address"] for row in indexability] == [
        "https://example.com/missing",
        "https://example.com/error",
    ]
    assert indexability[0]["Meta Robots"] == "noindex"
    assert indexability[1]["X-Robots-Tag"] == "noindex"


def test_redirect_chain_report_collects_rows_from_tab(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/a", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "redirect_chains.csv",
        ["Address", "Number of Redirects", "Loop"],
        [
            {"Address": "https://example.com/a", "Number of Redirects": "2", "Loop": "false"},
            {"Address": "https://example.com/b", "Number of Redirects": "4", "Loop": "true"},
        ],
    )

    crawl = Crawl.load(str(tmp_path))

    report = crawl.redirect_chain_report(min_hops=3)

    assert report == [
        {"Address": "https://example.com/b", "Number of Redirects": "4", "Loop": "true"}
    ]


def test_link_and_orphan_reports_from_csv(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        ["Address", "Status Code", "Title 1", "Indexability", "Indexability Status"],
        [
            {
                "Address": "https://example.com/home",
                "Status Code": "200",
                "Title 1": "Home",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
            },
            {
                "Address": "https://example.com/orphan",
                "Status Code": "200",
                "Title 1": "Orphan",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
            },
            {
                "Address": "https://example.com/noindex-orphan",
                "Status Code": "200",
                "Title 1": "Hidden",
                "Indexability": "Non-Indexable",
                "Indexability Status": "Noindex",
            },
        ],
    )
    _write_csv(
        tmp_path / "all_inlinks.csv",
        ["Address", "Source", "Status Code", "Follow", "Rel"],
        [
            {
                "Address": "https://example.com/home",
                "Source": "https://example.com/nav",
                "Status Code": "200",
                "Follow": "follow",
                "Rel": "",
            },
            {
                "Address": "https://example.com/broken",
                "Source": "https://example.com/nav",
                "Status Code": "404",
                "Follow": "follow",
                "Rel": "",
            },
            {
                "Address": "https://example.com/sponsored",
                "Source": "https://example.com/nav",
                "Status Code": "200",
                "Follow": "nofollow",
                "Rel": "nofollow sponsored",
            },
            {
                "Address": "https://example.com/self",
                "Source": "https://example.com/self",
                "Status Code": "200",
                "Follow": "follow",
                "Rel": "",
            },
        ],
    )
    _write_csv(tmp_path / "all_outlinks.csv", ["Source", "Destination"], [])

    crawl = Crawl.load(str(tmp_path))

    broken = crawl.broken_inlinks_report()
    nofollow = crawl.nofollow_inlinks_report()
    orphans = crawl.orphan_pages_report()
    indexable_orphans = crawl.orphan_pages_report(only_indexable=True)

    assert broken == [
        {
            "Address": "https://example.com/broken",
            "Source": "https://example.com/nav",
            "Status Code": "404",
            "Follow": "follow",
            "Rel": "",
        }
    ]
    assert nofollow == [
        {
            "Address": "https://example.com/sponsored",
            "Source": "https://example.com/nav",
            "Status Code": "200",
            "Follow": "nofollow",
            "Rel": "nofollow sponsored",
        }
    ]
    assert [row["Address"] for row in orphans] == [
        "https://example.com/orphan",
        "https://example.com/noindex-orphan",
    ]
    assert [row["Address"] for row in indexable_orphans] == ["https://example.com/orphan"]


def test_issue_reports_collect_rows_from_available_tabs(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "security_missing_hsts_header.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/no-hsts", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "canonicals_missing.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/no-canonical", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "hreflang_missing_return_links.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/no-return", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "response_codes_internal_redirect_chain.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/chain", "Status Code": "301"}],
    )

    crawl = Crawl.load(str(tmp_path))

    security = crawl.security_issues_report()
    canonical = crawl.canonical_issues_report()
    hreflang = crawl.hreflang_issues_report()
    redirects = crawl.redirect_issues_report()

    assert security == [
        {
            "Address": "https://example.com/no-hsts",
            "Status Code": "200",
            "Issue": "Missing HSTS Header",
        }
    ]
    assert canonical == [
        {
            "Address": "https://example.com/no-canonical",
            "Status Code": "200",
            "Issue": "Missing Canonical",
        }
    ]
    assert hreflang == [
        {
            "Address": "https://example.com/no-return",
            "Status Code": "200",
            "Issue": "Missing Return Links",
        }
    ]
    assert redirects == [
        {
            "Address": "https://example.com/chain",
            "Status Code": "301",
            "Issue": "Redirect Chain",
        }
    ]


def test_summary_rolls_up_key_issue_counts(tmp_path: Path) -> None:
    _write_csv(
        tmp_path / "internal_all.csv",
        [
            "Address",
            "Status Code",
            "Title 1",
            "Meta Description 1",
            "Indexability",
            "Indexability Status",
        ],
        [
            {
                "Address": "https://example.com/home",
                "Status Code": "200",
                "Title 1": "Home",
                "Meta Description 1": "Desc",
                "Indexability": "Indexable",
                "Indexability Status": "Indexable",
            },
            {
                "Address": "https://example.com/orphan",
                "Status Code": "404",
                "Title 1": "",
                "Meta Description 1": "",
                "Indexability": "Non-Indexable",
                "Indexability Status": "Noindex",
            },
        ],
    )
    _write_csv(
        tmp_path / "all_inlinks.csv",
        ["Address", "Source", "Status Code", "Follow", "Rel"],
        [
            {
                "Address": "https://example.com/broken-target",
                "Source": "https://example.com/home",
                "Status Code": "404",
                "Follow": "nofollow",
                "Rel": "nofollow",
            }
        ],
    )
    _write_csv(tmp_path / "all_outlinks.csv", ["Source", "Destination"], [])
    _write_csv(
        tmp_path / "redirect_chains.csv",
        ["Address", "Number of Redirects", "Loop"],
        [{"Address": "https://example.com/redirect", "Number of Redirects": "2", "Loop": "false"}],
    )
    _write_csv(
        tmp_path / "security_missing_hsts_header.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/home", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "canonicals_missing.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/orphan", "Status Code": "404"}],
    )
    _write_csv(
        tmp_path / "hreflang_missing_return_links.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/home", "Status Code": "200"}],
    )
    _write_csv(
        tmp_path / "response_codes_internal_redirect_chain.csv",
        ["Address", "Status Code"],
        [{"Address": "https://example.com/redirect", "Status Code": "301"}],
    )

    crawl = Crawl.load(str(tmp_path))

    summary = crawl.summary()

    assert summary == {
        "pages": 2,
        "tabs": 8,
        "broken_pages": 1,
        "broken_inlinks": 1,
        "nofollow_inlinks": 1,
        "orphan_pages": 2,
        "non_indexable_pages": 1,
        "redirect_chains": 1,
        "security_issues": 1,
        "canonical_issues": 1,
        "hreflang_issues": 1,
        "redirect_issues": 2,
    }
