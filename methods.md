# Methods Reference (Alpha)

This file lists the current callable API in `sf-alpha`.

## 1) Top-level API (`from screamingfrog import ...`)

### Crawl + analysis
- `list_crawls(project_root=None) -> list[CrawlInfo]`
- `Crawl.load(path, ..., source_type="auto", ...) -> Crawl`
- `Crawl.from_exports(export_dir) -> Crawl`
- `Crawl.from_database(db_path) -> Crawl`
- `Crawl.from_duckdb(db_path) -> Crawl`
- `Crawl.from_derby(db_path, ..., csv_fallback=True, ...) -> Crawl`
- `Crawl.from_seospider(crawl_path, ..., backend="derby", ...) -> Crawl`
- `Crawl.from_db_id(crawl_id, ..., backend="derby", ...) -> Crawl`

- `crawl.internal` (property-like view object: `InternalView`)
- `crawl.tab(name) -> TabView`
- `crawl.pages() -> TabView`
- `crawl.links(direction="out") -> LinkView`
- `crawl.section(prefix) -> CrawlSection`
- `crawl.tabs -> list[str]`
- `crawl.inlinks(url) -> Iterator[Link]`
- `crawl.outlinks(url) -> Iterator[Link]`
- `crawl.redirect_chains(min_hops=None, max_hops=None, loop=None) -> Iterator[dict[str, Any]]`
- `crawl.canonical_chains(min_hops=None, max_hops=None, loop=None) -> Iterator[dict[str, Any]]`
- `crawl.redirect_and_canonical_chains(min_hops=None, max_hops=None, loop=None) -> Iterator[dict[str, Any]]`
- `crawl.broken_links_report(min_status=400, max_status=599, max_inlinks=25) -> list[dict[str, Any]]`
- `crawl.title_meta_audit() -> list[dict[str, Any]]`
- `crawl.indexability_audit() -> list[dict[str, Any]]`
- `crawl.redirect_chain_report(min_hops=None, max_hops=None, loop=None) -> list[dict[str, Any]]`
- `crawl.tab_filters(name) -> list[str]`
- `crawl.tab_filter_defs(name) -> list[Any]`
- `crawl.tab_columns(name) -> list[str]`
- `crawl.describe_tab(name) -> dict[str, Any]`
- `crawl.query(schema, table) -> QueryView`
- `crawl.raw(table) -> Iterator[dict[str, Any]]`
- `crawl.sql(query, params=None) -> Iterator[dict[str, Any]]`
- `crawl.export_duckdb(path, tables=None, tabs=None, if_exists="replace", source_label=None) -> Path`
- `crawl.compare(other, ..., title_fields=None, redirect_fields=None, redirect_type_fields=None, field_groups=None) -> CrawlDiff`

- `export_duckdb_from_derby(db_path, duckdb_path, ..., tables=None, tabs=None, if_exists="replace") -> Path`
- `export_duckdb_from_db_id(db_id, duckdb_path, ..., tables=None, tabs=None, if_exists="replace") -> Path`

### Config patching
- `ConfigPatches()`
- `CustomSearch(...)`
- `CustomJavaScript(...)`
- `write_seospider_config(template_config, output_config, patches, sf_path=None) -> Path`

### CLI wrapper
- `export_crawl(load_target, export_dir=None, ..., export_tabs=None, bulk_exports=None, save_reports=None, export_profile=None, ...) -> Path`
- `run_cli(args, cli_path=None, check=True) -> subprocess.CompletedProcess[str]`
- `start_crawl(start_url, output_dir, ..., config=None, auth_config=None, export_tabs=None, bulk_exports=None, save_reports=None, ...) -> subprocess.CompletedProcess[str]`

### `.dbseospider` packaging helpers
- `CrawlInfo` (dataclass)
- `pack_dbseospider(project_dir, output_file) -> Path`
- `pack_dbseospider_from_db_id(db_id, output_file, project_root=None) -> Path`
- `unpack_dbseospider(dbseospider_file, output_dir) -> Path`
- `export_dbseospider_from_seospider(crawl_path, output_file, ..., ensure_db_mode=True, cleanup_exports=True) -> Path`
- `load_seospider_db_project(crawl_path, ..., ensure_db_mode=True, cleanup_exports=True) -> Path`

---

## 2) View methods

## `InternalView` (returned by `crawl.internal`)
- `filter(**kwargs) -> InternalView`
- `count() -> int`
- `collect() -> list[InternalPage]`
- `first() -> InternalPage | None`
- `to_pandas()`
- `to_polars()`
- iterable (`for page in crawl.internal.filter(...): ...`)
- Derby-backed `crawl.internal` also materializes mapped expression fields such as `Indexability` and `Indexability Status`

## `TabView` (returned by `crawl.tab("...")`)
- `filter(**kwargs) -> TabView`
  - supports normal column filters
  - supports GUI filter shortcut via `gui="Missing"` or `gui_filters=[...]`
- `count() -> int`
- `collect() -> list[dict[str, Any]]`
- `first() -> dict[str, Any] | None`
- `to_pandas()`
- `to_polars()`
- iterable (`for row in crawl.tab("...").filter(...): ...`)

## `LinkView` (returned by `crawl.links(...)`)
- `filter(**kwargs) -> LinkView`
- `count() -> int`
- `collect() -> list[dict[str, Any]]`
- `first() -> dict[str, Any] | None`
- `to_pandas()`
- `to_polars()`
- iterable (`for row in crawl.links("in").filter(...): ...`)

## `CrawlSection` (returned by `crawl.section("...")`)
- `pages() -> ScopedRowView`
- `links(direction="out") -> ScopedRowView`
- prefix can be a full URL prefix or a path prefix like `/blog`

## `QueryView` (returned by `crawl.query("APP", "URLS")`)
- `select(*columns) -> QueryView`
- `where(sql_fragment, *params) -> QueryView`
- `group_by(*columns) -> QueryView`
- `having(sql_fragment, *params) -> QueryView`
- `order_by(*columns) -> QueryView`
- `limit(n) -> QueryView`
- `collect() -> list[dict[str, Any]]`
- `first() -> dict[str, Any] | None`
- `to_pandas()`
- `to_polars()`
- `to_sql() -> tuple[str, list[Any]]`

---

## 3) Config builder object methods

## `ConfigPatches`
- `set(path, value) -> ConfigPatches`
- `add_extraction(name, selector, selector_type="XPATH", extract_mode="TEXT", attribute=None) -> ConfigPatches`
- `remove_extraction(name) -> ConfigPatches`
- `clear_extractions() -> ConfigPatches`
- `add_custom_search(rule: CustomSearch) -> ConfigPatches`
- `remove_custom_search(name) -> ConfigPatches`
- `clear_custom_searches() -> ConfigPatches`
- `add_custom_javascript(rule: CustomJavaScript) -> ConfigPatches`
- `remove_custom_javascript(name) -> ConfigPatches`
- `clear_custom_javascript() -> ConfigPatches`
- `to_dict() -> dict[str, Any]`
- `to_json(indent=2) -> str`

## `CustomSearch`
- `to_op() -> dict[str, Any]`

## `CustomJavaScript`
- `to_op() -> dict[str, Any]`

---

## 4) Submodule APIs

## `screamingfrog.cli`
- `export_crawl(...)`
- `resolve_cli_path(cli_path=None) -> Path`
- `run_cli(...)`
- `start_crawl(...)`
- `ensure_storage_mode(mode="DB", config_path=None)` (context manager)
- `resolve_spider_config(config_path=None) -> Path`

## `screamingfrog.config`
- `get_export_profile(name="kitchen_sink") -> ExportProfile`
- `write_seospider_config(...)`
- plus classes: `ConfigPatches`, `CustomSearch`, `CustomJavaScript`, `ExportProfile`

## `screamingfrog.db`
- `connect(db_path) -> sqlite3.Connection`
- `list_crawls(project_root=None) -> list[CrawlInfo]`
- `pack_dbseospider(...)`
- `pack_dbseospider_from_db_id(...)`
- `unpack_dbseospider(...)`
- `export_dbseospider_from_seospider(...)`
- `load_seospider_db_project(...)`
- dataclass: `CrawlInfo`
- dataclass: `InternalRow`

---

## 5) Data model class methods

## `InternalPage`
- `InternalPage.from_csv_row(row) -> InternalPage`
- `InternalPage.from_db_row(columns, values) -> InternalPage`
- `InternalPage.from_data(data, copy_data=True) -> InternalPage`

## `Link`
- `Link.from_row(row) -> Link`

## Diff models (returned by `crawl.compare`)
- `CrawlDiff`
  - `summary() -> dict[str, int]`
  - `to_rows() -> list[dict[str, Any]]`
  - `to_pandas()`
  - `to_polars()`
- `StatusChange`
- `TitleChange`
- `RedirectChange`
- `FieldChange`

---

## 6) Chain report helpers

Dedicated helpers are available:

- `crawl.redirect_chains(...)`
- `crawl.canonical_chains(...)`
- `crawl.redirect_and_canonical_chains(...)`

They use the same underlying tab data and add ergonomic filtering by hop count and loop flag.

Equivalent tab access remains available:

- `crawl.tab("redirect_chains")`
- `crawl.tab("canonical_chains")`
- `crawl.tab("redirect_and_canonical_chains")`

Example (chain helper):

```python
rows = list(crawl.redirect_chains(min_hops=4, loop=False))
```

Example (raw tab style):

```python
rows = [
    r for r in crawl.tab("redirect_chains")
    if (r.get("Number of Redirects") or 0) > 3
]
```
