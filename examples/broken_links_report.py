from __future__ import annotations

import sys
from typing import Iterable

from screamingfrog import Crawl


def _iter_broken(crawl: Crawl) -> Iterable[dict[str, object]]:
    try:
        yield from crawl.tab("response_codes_internal_client_error_(4xx)")
        return
    except Exception:
        pass

    for page in crawl.internal.filter(status_code=404):
        yield {"Address": page.address, "Status Code": page.status_code}


def main() -> None:
    target = sys.argv[1] if len(sys.argv) > 1 else "./crawl.dbseospider"
    crawl = Crawl.load(target)

    for row in _iter_broken(crawl):
        url = str(row.get("Address") or "")
        code = row.get("Status Code")
        if not url:
            continue
        print(f"{code}: {url}")
        try:
            inlinks = list(crawl.inlinks(url))
        except Exception:
            inlinks = []
        for link in inlinks[:25]:
            print(f"  <- {link.source} ({link.anchor_text or ''})")
        if inlinks and len(inlinks) > 25:
            print(f"  ... {len(inlinks) - 25} more")


if __name__ == "__main__":
    main()
