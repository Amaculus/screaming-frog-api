from __future__ import annotations

import sys
from typing import Iterable

from screamingfrog import Crawl


def _iter_missing_titles(crawl: Crawl) -> Iterable[str]:
    try:
        for row in crawl.tab("page_titles_missing"):
            address = row.get("Address")
            if address:
                yield str(address)
        return
    except Exception:
        pass

    for page in crawl.internal:
        title = page.data.get("Title 1") or page.data.get("Title") or page.data.get("title")
        if not title:
            yield page.address


def _iter_missing_meta(crawl: Crawl) -> Iterable[str]:
    try:
        for row in crawl.tab("meta_description_missing"):
            address = row.get("Address")
            if address:
                yield str(address)
        return
    except Exception:
        pass

    for page in crawl.internal:
        meta = page.data.get("Meta Description 1") or page.data.get("Meta Description")
        if not meta:
            yield page.address


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "./crawl.dbseospider"
    crawl = Crawl.load(target)

    print("Missing titles:")
    for url in _iter_missing_titles(crawl):
        print(f"  {url}")

    print("\nMissing meta descriptions:")
    for url in _iter_missing_meta(crawl):
        print(f"  {url}")


if __name__ == "__main__":
    main()
