# ConfigBuilder Patch Helpers

This document covers the ConfigPatches builder, CustomSearch rules, and CustomJavaScript extraction rules.

---

## Overview

The library includes helpers to build ConfigBuilder-compatible patch JSON payloads. These patches can be used to:

- Set arbitrary configuration options
- Add custom extraction rules (XPath, CSS, Regex)
- Define custom search rules
- Configure custom JavaScript extractions

---

## Core Classes

### ConfigPatches

The main builder class for creating patch payloads:

```python
from screamingfrog.config import ConfigPatches

# Create a new patch builder
patches = ConfigPatches()

# Chain multiple operations
patches = (
    ConfigPatches()
    .set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")
    .set("mCrawlConfig.mMaxUrls", 10000)
    .add_extraction("Product Price", "//span[@class='price']", "XPATH", "TEXT")
)

# Export to dictionary or JSON
payload = patches.to_dict()
json_str = patches.to_json(indent=2)
```

### CustomSearch

Define custom search rules that filter or highlight matching content:

```python
from screamingfrog.config import CustomSearch

# Basic text search
rule = CustomSearch(
    name="Filter 1",
    query="keyword",
    mode="CONTAINS",
    data_type="TEXT",
    scope="HTML",
)

# Regex search
regex_rule = CustomSearch(
    name="Phone Numbers",
    query=r"\d{3}-\d{3}-\d{4}",
    data_type="REGEX",
    scope="HTML",
    case_sensitive=False,
)

# XPath-scoped search
xpath_rule = CustomSearch(
    name="Product SKU",
    query="SKU-",
    data_type="TEXT",
    scope="XPATH",
    xpath="//div[@class='product-info']",
)
```

### CustomJavaScript

Define custom JavaScript extraction rules:

```python
from screamingfrog.config import CustomJavaScript

# Basic extraction
js_rule = CustomJavaScript(
    name="Extractor 1",
    javascript="return document.title;",
)

# With options
js_rule = CustomJavaScript(
    name="Product Data",
    javascript="""
        const product = document.querySelector('.product-data');
        return product ? product.dataset.sku : null;
    """,
    type="EXTRACTION",
    timeout_secs=15,
    content_types="text/html",
)
```

---

## ConfigPatches API

### Arbitrary Configuration

#### `set(path, value)`

Set any configuration key:

```python
patches = ConfigPatches()

# Rendering mode
patches.set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")

# Max URLs
patches.set("mCrawlConfig.mMaxUrls", 50000)

# Max crawl depth
patches.set("mCrawlConfig.mMaxCrawlDepth", 10)

# User agent
patches.set("mCrawlConfig.mUserAgent", "Custom Bot/1.0")

# Respect robots.txt
patches.set("mCrawlConfig.mRespectRobots", True)

# Crawl speed
patches.set("mCrawlConfig.mCrawlSpeed", 5)  # 1-5 scale
```

### Custom Extractions

#### `add_extraction(name, selector, selector_type, extract_mode, attribute)`

Add a custom extraction rule:

```python
patches = ConfigPatches()

# XPath text extraction
patches.add_extraction(
    name="Product Name",
    selector="//h1[@class='product-title']",
    selector_type="XPATH",
    extract_mode="TEXT",
)

# CSS selector extraction
patches.add_extraction(
    name="Price",
    selector=".price-current",
    selector_type="CSS",
    extract_mode="TEXT",
)

# Extract attribute value
patches.add_extraction(
    name="Product ID",
    selector="//div[@data-product-id]",
    selector_type="XPATH",
    extract_mode="TEXT",
    attribute="data-product-id",
)

# Extract inner HTML
patches.add_extraction(
    name="Description HTML",
    selector="//div[@class='description']",
    selector_type="XPATH",
    extract_mode="INNER_HTML",
)
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Column name for extracted data |
| `selector` | str | required | XPath or CSS selector |
| `selector_type` | str | "XPATH" | "XPATH" or "CSS" |
| `extract_mode` | str | "TEXT" | "TEXT", "INNER_HTML", or "HTML_ELEMENT" |
| `attribute` | str | None | Attribute name to extract |

#### `remove_extraction(name)`

Remove a specific extraction by name:

```python
patches.remove_extraction("Product Name")
```

#### `clear_extractions()`

Remove all extractions:

```python
patches.clear_extractions()
```

---

### Custom Searches

#### `add_custom_search(rule)`

Add a custom search rule:

```python
from screamingfrog.config import ConfigPatches, CustomSearch

patches = ConfigPatches()

# Text contains search
patches.add_custom_search(CustomSearch(
    name="Contains Keyword",
    query="special offer",
    mode="CONTAINS",
    data_type="TEXT",
    scope="HTML",
))

# Regex search
patches.add_custom_search(CustomSearch(
    name="Email Addresses",
    query=r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    data_type="REGEX",
    scope="HTML",
))

# Case-sensitive search
patches.add_custom_search(CustomSearch(
    name="Exact Match",
    query="ProductX",
    mode="CONTAINS",
    data_type="TEXT",
    case_sensitive=True,
))
```

#### `remove_custom_search(name)`

Remove a specific search by name:

```python
patches.remove_custom_search("Contains Keyword")
```

#### `clear_custom_searches()`

Remove all custom searches:

```python
patches.clear_custom_searches()
```

---

### Custom JavaScript

#### `add_custom_javascript(rule)`

Add a custom JavaScript extraction:

```python
from screamingfrog.config import ConfigPatches, CustomJavaScript

patches = ConfigPatches()

# Simple extraction
patches.add_custom_javascript(CustomJavaScript(
    name="Page Title",
    javascript="return document.title;",
))

# Complex extraction
patches.add_custom_javascript(CustomJavaScript(
    name="Schema.org Data",
    javascript="""
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        const data = [];
        scripts.forEach(s => {
            try { data.push(JSON.parse(s.textContent)); } catch(e) {}
        });
        return JSON.stringify(data);
    """,
    timeout_secs=20,
))

# Filter type (not extraction)
patches.add_custom_javascript(CustomJavaScript(
    name="Has Video",
    javascript="return document.querySelector('video') !== null;",
    type="FILTER",
))
```

#### `remove_custom_javascript(name)`

Remove a specific JavaScript rule by name:

```python
patches.remove_custom_javascript("Page Title")
```

#### `clear_custom_javascript()`

Remove all custom JavaScript rules:

```python
patches.clear_custom_javascript()
```

---

## CustomSearch Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Rule name (column header) |
| `query` | str | required | Search query or pattern |
| `mode` | str | "CONTAINS" | Match mode: "CONTAINS", "EXACT", "DOES_NOT_CONTAIN" |
| `data_type` | str | "TEXT" | Query type: "TEXT", "REGEX" |
| `scope` | str | "HTML" | Search scope: "HTML", "URL", "XPATH" |
| `case_sensitive` | bool | False | Case-sensitive matching |
| `xpath` | str | None | XPath for scoped search (when scope="XPATH") |

---

## CustomJavaScript Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | str | required | Rule name (column header) |
| `javascript` | str | required | JavaScript code to execute |
| `type` | str | "EXTRACTION" | "EXTRACTION" or "FILTER" |
| `timeout_secs` | int | 10 | Execution timeout in seconds |
| `content_types` | str | "text/html" | Content types to process |

---

## Output Format

### `to_dict()`

Returns the patch payload as a Python dictionary:

```python
patches = (
    ConfigPatches()
    .set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")
    .add_extraction("Title", "//title", "XPATH", "TEXT")
)

payload = patches.to_dict()
# {
#     "mCrawlConfig.mRenderingMode": "JAVASCRIPT",
#     "extractions": [
#         {
#             "op": "add",
#             "name": "Title",
#             "selector": "//title",
#             "selectorType": "XPATH",
#             "extractMode": "TEXT"
#         }
#     ]
# }
```

### `to_json(indent=2)`

Returns the patch payload as a JSON string:

```python
json_str = patches.to_json(indent=2)
print(json_str)
```

---

## Complete Examples

### E-commerce Crawl Configuration

```python
from screamingfrog.config import ConfigPatches, CustomSearch, CustomJavaScript

patches = (
    ConfigPatches()
    # Rendering
    .set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")
    .set("mCrawlConfig.mJsRenderingTimeout", 10000)

    # Crawl limits
    .set("mCrawlConfig.mMaxUrls", 100000)
    .set("mCrawlConfig.mMaxCrawlDepth", 15)

    # Product extractions
    .add_extraction("Product Name", "//h1[@class='product-title']", "XPATH", "TEXT")
    .add_extraction("Price", "//span[@class='price']", "XPATH", "TEXT")
    .add_extraction("SKU", "//span[@data-sku]", "XPATH", "TEXT", attribute="data-sku")
    .add_extraction("Availability", "//div[@class='stock-status']", "XPATH", "TEXT")

    # Custom search for out-of-stock
    .add_custom_search(CustomSearch(
        name="Out of Stock",
        query="out of stock",
        mode="CONTAINS",
        scope="HTML",
        case_sensitive=False,
    ))

    # JavaScript extraction for schema data
    .add_custom_javascript(CustomJavaScript(
        name="Product Schema",
        javascript="""
            const script = document.querySelector('script[type="application/ld+json"]');
            if (script) {
                try {
                    const data = JSON.parse(script.textContent);
                    if (data['@type'] === 'Product') return JSON.stringify(data);
                } catch(e) {}
            }
            return null;
        """,
        timeout_secs=15,
    ))
)

# Export for use with ConfigBuilder
config_json = patches.to_json()
```

### Content Audit Configuration

```python
from screamingfrog.config import ConfigPatches, CustomSearch

patches = (
    ConfigPatches()
    # Basic settings
    .set("mCrawlConfig.mRenderingMode", "TEXT")
    .set("mCrawlConfig.mMaxUrls", 50000)

    # Content extractions
    .add_extraction("Author", "//meta[@name='author']", "XPATH", "TEXT", attribute="content")
    .add_extraction("Publish Date", "//meta[@property='article:published_time']", "XPATH", "TEXT", attribute="content")
    .add_extraction("Category", "//meta[@property='article:section']", "XPATH", "TEXT", attribute="content")

    # Search for content issues
    .add_custom_search(CustomSearch(
        name="Lorem Ipsum",
        query="lorem ipsum",
        mode="CONTAINS",
        scope="HTML",
        case_sensitive=False,
    ))
    .add_custom_search(CustomSearch(
        name="TODO Comments",
        query=r"TODO|FIXME|XXX",
        data_type="REGEX",
        scope="HTML",
    ))
)

print(patches.to_json())
```

### Migration Audit Configuration

```python
from screamingfrog.config import ConfigPatches, CustomJavaScript

patches = (
    ConfigPatches()
    .set("mCrawlConfig.mFollowRedirects", True)
    .set("mCrawlConfig.mMaxRedirects", 10)

    # Track old URL patterns
    .add_custom_search(CustomSearch(
        name="Old Domain Links",
        query="old-domain.com",
        mode="CONTAINS",
        scope="HTML",
    ))

    # Extract redirect chains
    .add_custom_javascript(CustomJavaScript(
        name="Final URL",
        javascript="return window.location.href;",
    ))
)
```

---

## Integration with SF ConfigBuilder

The patch JSON output is compatible with the Screaming Frog ConfigBuilder patch format. Use it with:

1. **MCP Server**: Pass to the `sf_config_build` tool
2. **CLI**: Use with `--config-patch` argument
3. **API**: Include in configuration API calls

```python
# Generate patch
patches = ConfigPatches().set("mCrawlConfig.mMaxUrls", 10000)
patch_json = patches.to_json()

# Use with MCP server sf_config_build
# The patch JSON can be passed as the "patches" parameter
```

---

## Chaining

All methods return `self` for fluent chaining:

```python
patches = (
    ConfigPatches()
    .set("key1", "value1")
    .set("key2", "value2")
    .add_extraction("E1", "//xpath1", "XPATH", "TEXT")
    .add_extraction("E2", "//xpath2", "XPATH", "TEXT")
    .add_custom_search(CustomSearch(name="S1", query="test"))
    .add_custom_javascript(CustomJavaScript(name="JS1", javascript="return 1;"))
)
```

---

## Export Profiles

The library also includes export profile helpers:

```python
from screamingfrog.config import get_export_profile, ExportProfile

# Get the kitchen_sink profile
profile = get_export_profile("kitchen_sink")

print(f"Export tabs: {len(profile.export_tabs)}")
print(f"Bulk exports: {len(profile.bulk_exports)}")

# Access lists
for tab in profile.export_tabs[:5]:
    print(f"  - {tab}")
```

### ExportProfile

```python
@dataclass(frozen=True)
class ExportProfile:
    export_tabs: list[str]      # List of Tab:Filter labels
    bulk_exports: list[str]     # List of bulk export labels
```

---

## Limitations

- **Read-only patches**: This builds patch payloads; it doesn't apply them directly
- **Validation**: Limited validation of selector syntax or JavaScript code
- **SF version**: Some config keys may vary by Screaming Frog version
- **Type coercion**: Values are passed as-is; ensure correct types for SF

