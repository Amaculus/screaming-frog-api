# Python Recipe Snippets

## Broken Internal Destinations
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
broken = crawl.internal.filter(status_code=404)
rows = [{
    'address': page.address,
    'status_code': page.status_code,
    'title': page.title_1,
} for page in broken]
```

## Redirect Chain Watchlist
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
chains = list(crawl.redirect_chains(min_hops=2))
```

## Crawl Over Crawl Regression Check
```python
from screamingfrog import Crawl

before = Crawl.load('baseline.dbseospider')
after = Crawl.load('current.dbseospider')
diff = after.compare(before)

if diff.status_changes or diff.redirect_changes:
    raise SystemExit('crawl regression detected')
```

## Raw SQL Escape Hatch
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
for row in crawl.sql('''
    SELECT ENCODED_URL, RESPONSE_CODE
    FROM APP.URLS
    WHERE IS_INTERNAL = 1 AND RESPONSE_CODE >= 400
    FETCH FIRST 20 ROWS ONLY
'''):
    print(row)
```

## Inlink Audit For One URL
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
rows = list(crawl.inlinks('https://example.com/target-page'))
```

## Generic Tab Access
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
columns = crawl.tab_columns('internal_all')
pages = list(crawl.tab('internal_all').filter(indexability='Indexable'))
```

## Chainable Query Builder
```python
from screamingfrog import Crawl

crawl = Crawl.load('crawl.dbseospider')
rows = (
    crawl.query('APP.URLS')
    .select('ENCODED_URL', 'RESPONSE_CODE', 'TITLE_1')
    .where('IS_INTERNAL = 1')
    .where('RESPONSE_CODE >= 400')
    .limit(50)
    .collect()
)
```
