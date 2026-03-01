from __future__ import annotations

import sys

from screamingfrog import Crawl


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python crawl_diff.py <old_crawl> <new_crawl>")
        return

    old_path, new_path = sys.argv[1], sys.argv[2]
    old = Crawl.load(old_path)
    new = Crawl.load(new_path)

    diff = new.compare(old)

    print(f"Added: {len(diff.added_pages)}")
    print(f"Removed: {len(diff.removed_pages)}")
    print(f"Status changes: {len(diff.status_changes)}")
    print(f"Title changes: {len(diff.title_changes)}")
    print(f"Redirect changes: {len(diff.redirect_changes)}")
    print(f"Field changes: {len(diff.field_changes)}")

    for change in diff.status_changes[:10]:
        print(f"STATUS {change.url} {change.old_status} -> {change.new_status}")

    for change in diff.field_changes[:10]:
        print(f"FIELD {change.field} {change.url} {change.old_value} -> {change.new_value}")


if __name__ == "__main__":
    main()
