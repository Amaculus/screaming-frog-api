# Integration Recipe Snippets

## Slack Alert On Crawl Diff
```python
from screamingfrog import Crawl
import requests

before = Crawl.load('baseline.dbseospider')
after = Crawl.load('current.dbseospider')
diff = after.compare(before)

if diff.status_changes or diff.redirect_changes:
    requests.post(
        'https://hooks.slack.com/services/...',
        json={'text': f'Crawl regressions: {len(diff.status_changes)} status, {len(diff.redirect_changes)} redirects'}
    )
```

## Write Raw Query Results To DuckDB Or Warehouse
```python
from screamingfrog import Crawl
import pandas as pd

crawl = Crawl.load('crawl.dbseospider')
rows = list(crawl.sql('SELECT ENCODED_URL, RESPONSE_CODE, TITLE_1 FROM APP.URLS'))
df = pd.DataFrame(rows)
# df.to_parquet(...)
# df.to_sql(...)
```

## Jira Ticket Payload For New 4xx URLs
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
rows = [page.address for page in crawl.internal.filter(status_code=404)]
summary = f'New 404 URLs detected: {len(rows)}'
description = '\n'.join(rows[:50])
```

## MCP Tool Surface Concept
```python
# tool: crawl_broken_links
# input: {"crawl_path": "...", "limit": 50}
# action:
#   - load crawl
#   - query internal 4xx/5xx
#   - return sample URLs + counts
```

## Scheduled Monthly Client Report
```python
from screamingfrog import Crawl

crawl = Crawl.load('client-march.dbseospider')
report = {
    'broken_internal': crawl.internal.filter(status_code=404).count(),
    'redirect_chains': len(list(crawl.redirect_chains(min_hops=2))),
    'non_indexable': crawl.internal.filter(indexability='Non-Indexable').count(),
}
```
