# Golden Path Examples

This document provides complete, production-ready scripts for common SEO audit tasks.

---

## Overview

These examples demonstrate the recommended patterns for common workflows. Each script is designed to:

- Work with multiple backends
- Handle errors gracefully
- Provide useful output
- Be easily customizable

---

## Example Scripts Location

```
examples/
├── broken_links_report.py      # Find broken links + inlink sources
├── title_meta_audit.py         # Audit titles and meta descriptions
└── crawl_diff.py               # Compare two crawls
```

---

## 1. Broken Links Report

Find all broken links (4xx errors) and show which pages link to them.

### Script: `examples/broken_links_report.py`

```python
#!/usr/bin/env python
"""Broken Links Report - Find 4xx errors and their inlink sources."""

from __future__ import annotations

import sys
from typing import Iterable

from screamingfrog import Crawl


def _iter_broken(crawl: Crawl) -> Iterable[dict[str, object]]:
    """Get broken pages, preferring tab if available."""
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
```

### Usage

```bash
python examples/broken_links_report.py ./crawl.dbseospider
```

### Output

```
404: https://example.com/old-page
  <- https://example.com/about (Learn more)
  <- https://example.com/blog/post-1 (click here)
  <- https://example.com/resources (Old Page)
  ... 12 more
404: https://example.com/deleted-product
  <- https://example.com/products (View Product)
```

---

## 2. Title & Meta Description Audit

Find pages with missing or problematic titles and meta descriptions.

### Script: `examples/title_meta_audit.py`

```python
#!/usr/bin/env python
"""Title & Meta Description Audit - Find missing or problematic content."""

from __future__ import annotations

import sys
from typing import Iterable

from screamingfrog import Crawl


def _iter_missing_titles(crawl: Crawl) -> Iterable[str]:
    """Get pages with missing titles."""
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
    """Get pages with missing meta descriptions."""
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
```

### Usage

```bash
python examples/title_meta_audit.py ./crawl.dbseospider
```

### Output

```
Missing titles:
  https://example.com/product/12345
  https://example.com/api/callback

Missing meta descriptions:
  https://example.com/about
  https://example.com/contact
  https://example.com/terms
```

---

## 3. Crawl Diff Report

Compare two crawls and report changes.

### Script: `examples/crawl_diff.py`

```python
#!/usr/bin/env python
"""Crawl Diff Report - Compare two crawls and show changes."""

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
```

### Usage

```bash
python examples/crawl_diff.py ./old_crawl.dbseospider ./new_crawl.dbseospider
```

### Output

```
Added: 45
Removed: 12
Status changes: 8
Title changes: 23
Redirect changes: 5
Field changes: 156
STATUS https://example.com/page 200 -> 404
STATUS https://example.com/old 301 -> 200
FIELD Canonical https://example.com/about None -> https://example.com/about-us
```

---

## 4. Full Technical Audit

Comprehensive technical SEO audit script.

```python
#!/usr/bin/env python
"""Full Technical SEO Audit - Comprehensive site analysis."""

from __future__ import annotations

import csv
import sys
from collections import Counter
from dataclasses import dataclass
from typing import List

from screamingfrog import Crawl


@dataclass
class AuditResults:
    total_pages: int
    indexable_pages: int
    non_indexable_pages: int
    status_distribution: dict
    missing_titles: List[str]
    missing_meta: List[str]
    duplicate_titles: List[str]
    broken_links: List[str]
    orphan_pages: List[str]


def run_audit(crawl: Crawl) -> AuditResults:
    """Run comprehensive audit and return results."""

    # Basic counts
    total_pages = crawl.internal.count()
    indexable = list(crawl.internal.filter(indexability="Indexable"))
    non_indexable = list(crawl.internal.filter(indexability="Non-Indexable"))

    # Status distribution
    status_counter = Counter()
    for page in crawl.internal:
        status_counter[page.status_code] += 1

    # Missing titles
    missing_titles = []
    try:
        for row in crawl.tab("page_titles_missing"):
            if row.get("Address"):
                missing_titles.append(str(row["Address"]))
    except Exception:
        for page in crawl.internal:
            if not (page.data.get("Title 1") or page.data.get("Title")):
                missing_titles.append(page.address)

    # Missing meta descriptions
    missing_meta = []
    try:
        for row in crawl.tab("meta_description_missing"):
            if row.get("Address"):
                missing_meta.append(str(row["Address"]))
    except Exception:
        for page in crawl.internal:
            if not (page.data.get("Meta Description 1") or page.data.get("Meta Description")):
                missing_meta.append(page.address)

    # Duplicate titles
    duplicate_titles = []
    try:
        for row in crawl.tab("page_titles_duplicate"):
            if row.get("Address"):
                duplicate_titles.append(str(row["Address"]))
    except Exception:
        pass  # Requires duplicate detection logic

    # Broken links
    broken_links = []
    try:
        for row in crawl.tab("response_codes_internal_client_error_(4xx)"):
            if row.get("Address"):
                broken_links.append(str(row["Address"]))
    except Exception:
        for page in crawl.internal.filter(status_code=404):
            broken_links.append(page.address)

    # Orphan pages (no inlinks)
    orphan_pages = []
    try:
        for page in crawl.internal.filter(content_type="text/html"):
            inlinks = list(crawl.inlinks(page.address))
            if len(inlinks) == 0:
                orphan_pages.append(page.address)
    except Exception:
        pass  # Requires link graph support

    return AuditResults(
        total_pages=total_pages,
        indexable_pages=len(indexable),
        non_indexable_pages=len(non_indexable),
        status_distribution=dict(status_counter),
        missing_titles=missing_titles,
        missing_meta=missing_meta,
        duplicate_titles=duplicate_titles,
        broken_links=broken_links,
        orphan_pages=orphan_pages,
    )


def print_report(results: AuditResults) -> None:
    """Print formatted audit report."""
    print("=" * 60)
    print("TECHNICAL SEO AUDIT REPORT")
    print("=" * 60)

    print(f"\nTotal Pages: {results.total_pages}")
    print(f"Indexable: {results.indexable_pages}")
    print(f"Non-Indexable: {results.non_indexable_pages}")

    print("\nStatus Code Distribution:")
    for status, count in sorted(results.status_distribution.items()):
        print(f"  {status}: {count}")

    print(f"\nMissing Titles: {len(results.missing_titles)}")
    for url in results.missing_titles[:5]:
        print(f"  - {url}")
    if len(results.missing_titles) > 5:
        print(f"  ... and {len(results.missing_titles) - 5} more")

    print(f"\nMissing Meta Descriptions: {len(results.missing_meta)}")
    for url in results.missing_meta[:5]:
        print(f"  - {url}")
    if len(results.missing_meta) > 5:
        print(f"  ... and {len(results.missing_meta) - 5} more")

    print(f"\nDuplicate Titles: {len(results.duplicate_titles)}")
    print(f"Broken Links (4xx): {len(results.broken_links)}")
    print(f"Orphan Pages: {len(results.orphan_pages)}")

    print("\n" + "=" * 60)


def export_csv(results: AuditResults, prefix: str) -> None:
    """Export results to CSV files."""

    # Missing titles
    if results.missing_titles:
        with open(f"{prefix}_missing_titles.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["URL"])
            for url in results.missing_titles:
                writer.writerow([url])

    # Missing meta
    if results.missing_meta:
        with open(f"{prefix}_missing_meta.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["URL"])
            for url in results.missing_meta:
                writer.writerow([url])

    # Broken links
    if results.broken_links:
        with open(f"{prefix}_broken_links.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["URL"])
            for url in results.broken_links:
                writer.writerow([url])

    print(f"\nCSV files exported with prefix: {prefix}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python full_audit.py <crawl_file> [export_prefix]")
        return

    crawl_path = sys.argv[1]
    export_prefix = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"Loading crawl: {crawl_path}")
    crawl = Crawl.load(crawl_path)

    print("Running audit...")
    results = run_audit(crawl)

    print_report(results)

    if export_prefix:
        export_csv(results, export_prefix)


if __name__ == "__main__":
    main()
```

### Usage

```bash
python full_audit.py ./crawl.dbseospider audit_results
```

---

## 5. Redirect Chain Analyzer

Find and analyze redirect chains.

```python
#!/usr/bin/env python
"""Redirect Chain Analyzer - Find and analyze redirect chains."""

from __future__ import annotations

import sys
from collections import defaultdict

from screamingfrog import Crawl


def find_redirect_chains(crawl: Crawl, max_hops: int = 10) -> list:
    """Find all redirect chains in the crawl."""
    chains = []

    # Get all redirecting pages
    redirects = {}
    for page in crawl.internal:
        if page.status_code in [301, 302, 307, 308]:
            target = page.data.get("Redirect URL") or page.data.get("Redirect URI")
            if target:
                redirects[page.address] = (target, page.status_code)

    # Build chains
    for start_url in redirects:
        chain = [(start_url, redirects[start_url][1])]
        current = redirects[start_url][0]

        while current in redirects and len(chain) < max_hops:
            chain.append((current, redirects[current][1]))
            current = redirects[current][0]

        # Add final destination
        chain.append((current, None))

        if len(chain) > 2:  # At least 2 redirects
            chains.append(chain)

    return chains


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python redirect_chains.py <crawl_file>")
        return

    crawl = Crawl.load(sys.argv[1])
    chains = find_redirect_chains(crawl)

    print(f"Found {len(chains)} redirect chains with 2+ hops:\n")

    for i, chain in enumerate(chains[:20], 1):
        print(f"Chain {i} ({len(chain) - 1} hops):")
        for url, status in chain:
            if status:
                print(f"  [{status}] {url}")
            else:
                print(f"  [END] {url}")
        print()

    if len(chains) > 20:
        print(f"... and {len(chains) - 20} more chains")


if __name__ == "__main__":
    main()
```

---

## 6. Internal Link Analysis

Analyze internal linking structure.

```python
#!/usr/bin/env python
"""Internal Link Analysis - Analyze site's internal linking structure."""

from __future__ import annotations

import sys
from collections import Counter

from screamingfrog import Crawl


def analyze_internal_links(crawl: Crawl) -> None:
    """Analyze internal linking patterns."""

    inlink_counts = Counter()
    outlink_counts = Counter()

    html_pages = list(crawl.internal.filter(content_type="text/html"))
    total = len(html_pages)

    print(f"Analyzing {total} HTML pages...")

    for i, page in enumerate(html_pages):
        if i % 100 == 0:
            print(f"  Progress: {i}/{total}")

        try:
            inlinks = list(crawl.inlinks(page.address))
            outlinks = list(crawl.outlinks(page.address))

            inlink_counts[page.address] = len(inlinks)
            outlink_counts[page.address] = len(outlinks)
        except Exception:
            pass

    # Statistics
    print("\n" + "=" * 60)
    print("INTERNAL LINK ANALYSIS")
    print("=" * 60)

    if inlink_counts:
        avg_inlinks = sum(inlink_counts.values()) / len(inlink_counts)
        print(f"\nAverage inlinks per page: {avg_inlinks:.1f}")

        print("\nTop 10 most linked pages:")
        for url, count in inlink_counts.most_common(10):
            print(f"  {count}: {url}")

        print("\nPages with fewest inlinks:")
        for url, count in inlink_counts.most_common()[-10:]:
            print(f"  {count}: {url}")

        orphans = [url for url, count in inlink_counts.items() if count == 0]
        print(f"\nOrphan pages (0 inlinks): {len(orphans)}")
        for url in orphans[:5]:
            print(f"  {url}")

    if outlink_counts:
        avg_outlinks = sum(outlink_counts.values()) / len(outlink_counts)
        print(f"\nAverage outlinks per page: {avg_outlinks:.1f}")

        print("\nTop 10 pages by outlink count:")
        for url, count in outlink_counts.most_common(10):
            print(f"  {count}: {url}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python link_analysis.py <crawl_file>")
        return

    crawl = Crawl.load(sys.argv[1])
    analyze_internal_links(crawl)


if __name__ == "__main__":
    main()
```

---

## 7. Content Quality Report

Analyze content quality metrics.

```python
#!/usr/bin/env python
"""Content Quality Report - Analyze content quality metrics."""

from __future__ import annotations

import sys
from collections import Counter

from screamingfrog import Crawl


def analyze_content(crawl: Crawl) -> None:
    """Analyze content quality metrics."""

    word_counts = []
    thin_content = []
    no_h1 = []
    no_h2 = []

    for page in crawl.internal.filter(content_type="text/html"):
        # Word count
        wc = page.data.get("Word Count") or page.data.get("word_count") or 0
        if isinstance(wc, str):
            try:
                wc = int(wc)
            except ValueError:
                wc = 0

        word_counts.append(wc)

        if wc < 300:
            thin_content.append((page.address, wc))

        # Headings
        h1 = page.data.get("H1-1") or page.data.get("H1_1") or page.data.get("H1 1")
        h2 = page.data.get("H2-1") or page.data.get("H2_1") or page.data.get("H2 1")

        if not h1:
            no_h1.append(page.address)
        if not h2:
            no_h2.append(page.address)

    # Report
    print("=" * 60)
    print("CONTENT QUALITY REPORT")
    print("=" * 60)

    if word_counts:
        avg_wc = sum(word_counts) / len(word_counts)
        print(f"\nAverage word count: {avg_wc:.0f}")
        print(f"Min word count: {min(word_counts)}")
        print(f"Max word count: {max(word_counts)}")

        # Distribution
        buckets = Counter()
        for wc in word_counts:
            if wc < 100:
                buckets["0-99"] += 1
            elif wc < 300:
                buckets["100-299"] += 1
            elif wc < 500:
                buckets["300-499"] += 1
            elif wc < 1000:
                buckets["500-999"] += 1
            elif wc < 2000:
                buckets["1000-1999"] += 1
            else:
                buckets["2000+"] += 1

        print("\nWord count distribution:")
        for bucket in ["0-99", "100-299", "300-499", "500-999", "1000-1999", "2000+"]:
            print(f"  {bucket}: {buckets[bucket]} pages")

    print(f"\nThin content (<300 words): {len(thin_content)} pages")
    for url, wc in thin_content[:5]:
        print(f"  {wc} words: {url}")
    if len(thin_content) > 5:
        print(f"  ... and {len(thin_content) - 5} more")

    print(f"\nMissing H1: {len(no_h1)} pages")
    for url in no_h1[:5]:
        print(f"  {url}")

    print(f"\nMissing H2: {len(no_h2)} pages")
    for url in no_h2[:5]:
        print(f"  {url}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python content_report.py <crawl_file>")
        return

    crawl = Crawl.load(sys.argv[1])
    analyze_content(crawl)


if __name__ == "__main__":
    main()
```

---

## 8. Weekly Monitoring Script

Automated weekly crawl comparison.

```python
#!/usr/bin/env python
"""Weekly Monitoring - Compare this week's crawl to last week."""

from __future__ import annotations

import datetime
import json
import sys
from pathlib import Path

from screamingfrog import Crawl


def generate_report(diff, new_crawl) -> dict:
    """Generate structured report from diff."""
    return {
        "date": datetime.date.today().isoformat(),
        "summary": {
            "added_pages": len(diff.added_pages),
            "removed_pages": len(diff.removed_pages),
            "status_changes": len(diff.status_changes),
            "title_changes": len(diff.title_changes),
            "redirect_changes": len(diff.redirect_changes),
            "field_changes": len(diff.field_changes),
        },
        "alerts": [],
        "new_errors": [],
        "fixed_errors": [],
    }


def check_alerts(diff, report: dict) -> None:
    """Check for alertable conditions."""

    # Large number of removed pages
    if len(diff.removed_pages) > 50:
        report["alerts"].append(f"WARNING: {len(diff.removed_pages)} pages removed")

    # New 4xx/5xx errors
    new_errors = [
        c for c in diff.status_changes
        if c.old_status and c.old_status < 400 and c.new_status and c.new_status >= 400
    ]
    if new_errors:
        report["alerts"].append(f"CRITICAL: {len(new_errors)} new error pages")
        report["new_errors"] = [
            {"url": c.url, "old": c.old_status, "new": c.new_status}
            for c in new_errors[:20]
        ]

    # Fixed errors
    fixed = [
        c for c in diff.status_changes
        if c.old_status and c.old_status >= 400 and c.new_status == 200
    ]
    if fixed:
        report["alerts"].append(f"INFO: {len(fixed)} error pages fixed")
        report["fixed_errors"] = [
            {"url": c.url, "old": c.old_status, "new": c.new_status}
            for c in fixed[:20]
        ]


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python weekly_monitor.py <last_week.dbseospider> <this_week.dbseospider>")
        return

    old_path, new_path = sys.argv[1], sys.argv[2]

    print(f"Loading crawls...")
    old = Crawl.load(old_path)
    new = Crawl.load(new_path)

    print(f"Comparing crawls...")
    diff = new.compare(old)

    report = generate_report(diff, new)
    check_alerts(diff, report)

    # Print summary
    print("\n" + "=" * 60)
    print(f"WEEKLY MONITORING REPORT - {report['date']}")
    print("=" * 60)

    print(f"\nPages added: {report['summary']['added_pages']}")
    print(f"Pages removed: {report['summary']['removed_pages']}")
    print(f"Status changes: {report['summary']['status_changes']}")
    print(f"Title changes: {report['summary']['title_changes']}")

    if report["alerts"]:
        print("\nALERTS:")
        for alert in report["alerts"]:
            print(f"  {alert}")

    # Save report
    output_file = f"monitoring_report_{report['date']}.json"
    with open(output_file, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\nReport saved to: {output_file}")


if __name__ == "__main__":
    main()
```

---

## Common Patterns

### Error Handling

```python
# Try specific method, fall back to general
try:
    data = crawl.tab("specific_tab")
except Exception:
    data = crawl.internal.filter(...)
```

### Backend Detection

```python
# Check if feature is available
try:
    links = list(crawl.inlinks(url))
except NotImplementedError:
    print("Link graph not available with this backend")
```

### Progress Reporting

```python
total = crawl.internal.count()
for i, page in enumerate(crawl.internal):
    if i % 100 == 0:
        print(f"Progress: {i}/{total} ({i*100//total}%)")
```

### Memory-Efficient Processing

```python
# Stream processing
for page in crawl.internal:  # Generator
    process(page)

# Instead of
all_pages = list(crawl.internal)  # Loads all into memory
```

---

## Adapting Examples

### For Notebooks

```python
# In Jupyter notebook
from screamingfrog import Crawl

crawl = Crawl.load("./crawl.dbseospider")

# Use display() for better formatting
import pandas as pd

data = [{"URL": p.address, "Status": p.status_code} for p in crawl.internal]
df = pd.DataFrame(data)
display(df.head(20))
```

### For Pipelines

```python
# CI/CD integration
import sys

crawl = Crawl.load(sys.argv[1])
errors = list(crawl.internal.filter(status_code=404))

if len(errors) > 0:
    print(f"FAIL: {len(errors)} broken pages found")
    sys.exit(1)

print("PASS: No broken pages")
sys.exit(0)
```

### For Scheduled Jobs

```python
# Cron job wrapper
import datetime
import logging

logging.basicConfig(
    filename=f"audit_{datetime.date.today()}.log",
    level=logging.INFO
)

try:
    crawl = Crawl.load("./crawl.dbseospider")
    # ... run audit
    logging.info("Audit completed successfully")
except Exception as e:
    logging.error(f"Audit failed: {e}")
    raise
```

