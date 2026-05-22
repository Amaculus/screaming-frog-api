from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from screamingfrog import Crawl


def _time_call(fn: Callable[[], Any], repeat: int) -> dict[str, Any]:
    durations: list[float] = []
    result: Any = None
    for _ in range(repeat):
        start = time.perf_counter()
        result = fn()
        durations.append(time.perf_counter() - start)
    return {
        "min_s": min(durations),
        "median_s": statistics.median(durations),
        "max_s": max(durations),
        "last_result": result,
    }


def _address(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get("Address")
    return getattr(row, "address", None)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark common screamingfrog library operations.")
    parser.add_argument("crawl", help="Crawl path or DB crawl id accepted by Crawl.load().")
    parser.add_argument("--repeat", type=int, default=3, help="Iterations per operation.")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a text table.")
    args = parser.parse_args()

    repeat = max(1, int(args.repeat))
    load_start = time.perf_counter()
    crawl = Crawl.load(args.crawl)
    load_s = time.perf_counter() - load_start

    benchmarks: dict[str, Callable[[], Any]] = {
        "pages_count": lambda: crawl.pages().count(),
        "pages_first": lambda: _address(crawl.pages().first()),
        "internal_tab_count": lambda: crawl.tab_count("internal_all"),
        "internal_tab_first": lambda: (crawl.tab_rows("internal_all", limit=1) or [None])[0],
        "internal_tab_projection": lambda: crawl.tab("internal_all").select("Address", "Status Code").first(),
        "inlinks_count": lambda: crawl.tab_count("all_inlinks"),
        "inlinks_projection_first": lambda: crawl.tab("all_inlinks").select("Source", "Address").first(),
        "response_4xx_count": lambda: crawl.tab_count("response_codes_internal_client_error_(4xx)"),
        "response_4xx_first": lambda: (
            crawl.tab_rows("response_codes_internal_client_error_(4xx)", limit=1) or [None]
        )[0],
        "report_counts": lambda: crawl.report_counts(),
    }

    results: dict[str, Any] = {"crawl": str(Path(args.crawl)), "load_s": load_s, "repeat": repeat}
    for name, fn in benchmarks.items():
        try:
            results[name] = _time_call(fn, repeat)
        except Exception as exc:
            results[name] = {"error": f"{type(exc).__name__}: {exc}"}

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return

    print(f"load_s\t{load_s:.4f}")
    for name, value in results.items():
        if name in {"crawl", "load_s", "repeat"}:
            continue
        if "error" in value:
            print(f"{name}\tERROR\t{value['error']}")
            continue
        print(
            f"{name}\tmin={value['min_s']:.4f}s\tmedian={value['median_s']:.4f}s\tmax={value['max_s']:.4f}s"
        )


if __name__ == "__main__":
    main()
