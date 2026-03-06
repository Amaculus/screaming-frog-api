from __future__ import annotations

import csv
from pathlib import Path

import pytest

from screamingfrog import Crawl


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture()
def chain_export_dir(tmp_path: Path) -> Path:
    export_dir = tmp_path / "exports"
    export_dir.mkdir()

    _write_csv(
        export_dir / "redirect_chains.csv",
        ["Address", "Number of Redirects", "Loop"],
        [
            {"Address": "https://example.com/a", "Number of Redirects": "2", "Loop": "false"},
            {"Address": "https://example.com/b", "Number of Redirects": "4", "Loop": "false"},
            {"Address": "https://example.com/c", "Number of Redirects": "5", "Loop": "true"},
            {"Address": "https://example.com/d", "Number of Redirects": "", "Loop": "false"},
        ],
    )

    _write_csv(
        export_dir / "canonical_chains.csv",
        ["Address", "Number of Canonicals", "Loop"],
        [
            {"Address": "https://example.com/c1", "Number of Canonicals": "1", "Loop": "0"},
            {"Address": "https://example.com/c2", "Number of Canonicals": "3", "Loop": "1"},
        ],
    )

    _write_csv(
        export_dir / "redirect_and_canonical_chains.csv",
        ["Address", "Number of Redirects/Canonicals", "Loop"],
        [
            {"Address": "https://example.com/r1", "Number of Redirects/Canonicals": "2", "Loop": "false"},
            {"Address": "https://example.com/r2", "Number of Redirects/Canonicals": "6", "Loop": "true"},
        ],
    )

    return export_dir


def test_redirect_chains_filters_by_hops_and_loop(chain_export_dir: Path) -> None:
    crawl = Crawl.from_exports(str(chain_export_dir))

    all_rows = list(crawl.redirect_chains())
    assert len(all_rows) == 4

    rows = list(crawl.redirect_chains(min_hops=3, loop=False))
    assert [row["Address"] for row in rows] == ["https://example.com/b"]

    loop_rows = list(crawl.redirect_chains(loop=True))
    assert [row["Address"] for row in loop_rows] == ["https://example.com/c"]

    short_rows = list(crawl.redirect_chains(max_hops=2))
    assert [row["Address"] for row in short_rows] == ["https://example.com/a"]


def test_canonical_chain_and_mixed_chain_helpers_use_correct_hop_column(
    chain_export_dir: Path,
) -> None:
    crawl = Crawl.from_exports(str(chain_export_dir))

    canonical = list(crawl.canonical_chains(min_hops=2))
    assert [row["Address"] for row in canonical] == ["https://example.com/c2"]

    mixed = list(crawl.redirect_and_canonical_chains(min_hops=4))
    assert [row["Address"] for row in mixed] == ["https://example.com/r2"]


def test_chain_helpers_validate_hop_bounds(chain_export_dir: Path) -> None:
    crawl = Crawl.from_exports(str(chain_export_dir))

    with pytest.raises(ValueError, match="min_hops must be >= 0"):
        list(crawl.redirect_chains(min_hops=-1))

    with pytest.raises(ValueError, match="max_hops must be >= 0"):
        list(crawl.canonical_chains(max_hops=-1))

    with pytest.raises(ValueError, match="min_hops cannot be greater than max_hops"):
        list(crawl.redirect_and_canonical_chains(min_hops=5, max_hops=1))
