from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence
from urllib.parse import urljoin, urlsplit, urlunsplit
import re

from screamingfrog.backends import (
    CLIExportBackend,
    CSVBackend,
    CrawlBackend,
    DatabaseBackend,
    DerbyBackend,
    HybridBackend,
)
from screamingfrog.backends.hybrid_backend import FallbackConfig
from screamingfrog.db.derby import find_derby_db_root
from screamingfrog.db.packaging import find_project_dir, load_seospider_db_project, pack_dbseospider
from screamingfrog.filters.registry import list_filters as list_gui_filters
from screamingfrog.filters.names import normalize_name
from screamingfrog.models import InternalPage
from screamingfrog.models.diff import (
    CrawlDiff,
    FieldChange,
    RedirectChange,
    StatusChange,
    TitleChange,
)


_DEFAULT_FIELD_GROUPS: dict[str, Sequence[str]] = {
    "Canonical": (
        "Canonical Link Element 1",
        "Canonical Link Element",
        "CANONICAL_LINK_1",
        "CANONICAL_LINK_ELEMENT_1",
        "CANONICAL",
    ),
    "Canonical Status": (),
    "Meta Description": ("Meta Description 1", "Meta Description", "META_DESCRIPTION_1"),
    "Meta Keywords": ("Meta Keywords 1", "Meta Keywords", "META_KEYWORDS_1"),
    "Meta Refresh": (
        "Meta Refresh 1",
        "Meta Refresh",
        "META_REFRESH_1",
        "META_FULL_URL_1",
        "META_FULL_URL_2",
    ),
    "H1-1": ("H1-1", "H1 1", "H1", "H1_1"),
    "H2-1": ("H2-1", "H2 1", "H2", "H2_1"),
    "H3-1": ("H3-1", "H3 1", "H3", "H3_1"),
    "Word Count": ("Word Count", "WORD_COUNT"),
    "Indexability": ("Indexability", "INDEXABILITY"),
    "Indexability Status": ("Indexability Status", "INDEXABILITY_STATUS"),
    "Meta Robots": ("Meta Robots 1", "Meta Robots", "META_ROBOTS_1"),
    "X-Robots-Tag": ("X-Robots-Tag 1", "X-Robots-Tag", "X-Robots Tag", "X_ROBOTS_TAG_1"),
    "Directives Summary": (),
}


@dataclass(frozen=True)
class InternalView:
    backend: CrawlBackend
    filters: dict[str, Any] | None = None

    def filter(self, **kwargs: Any) -> "InternalView":
        merged = dict(self.filters or {})
        merged.update(kwargs)
        return InternalView(self.backend, merged)

    def __iter__(self) -> Iterator[InternalPage]:
        return self.backend.get_internal(filters=self.filters)

    def count(self) -> int:
        return self.backend.count("internal", filters=self.filters)


@dataclass(frozen=True)
class TabView:
    backend: CrawlBackend
    name: str
    filters: dict[str, Any] | None = None

    def filter(self, **kwargs: Any) -> "TabView":
        merged = dict(self.filters or {})
        gui = kwargs.pop("gui", None)
        gui_filters = kwargs.pop("gui_filters", None)
        if gui is not None:
            merged["__gui__"] = gui
        if gui_filters is not None:
            merged["__gui__"] = gui_filters
        merged.update(kwargs)
        return TabView(self.backend, self.name, merged)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.backend.get_tab(self.name, filters=self.filters)

    def count(self) -> int:
        return sum(1 for _ in self.__iter__())


class Crawl:
    """Unified crawl API that abstracts backend differences."""

    def __init__(self, backend: CrawlBackend):
        self._backend = backend
        self.internal = InternalView(backend)

    @classmethod
    def from_exports(cls, export_dir: str) -> "Crawl":
        return cls(CSVBackend(export_dir))

    @classmethod
    def from_database(cls, db_path: str) -> "Crawl":
        return cls(DatabaseBackend(db_path))

    @classmethod
    def from_derby(
        cls,
        db_path: str,
        mapping_path: str | None = None,
        derby_jar: str | None = None,
        *,
        csv_fallback: bool = True,
        csv_fallback_cache_dir: str | None = None,
        csv_fallback_profile: str = "kitchen_sink",
        csv_fallback_warn: bool = True,
        cli_path: str | None = None,
        export_format: str = "csv",
        headless: bool = True,
        overwrite: bool = False,
    ) -> "Crawl":
        derby = DerbyBackend(db_path, mapping_path=mapping_path, derby_jar=derby_jar)
        if not csv_fallback:
            return cls(derby)
        cache_dir = (
            Path(csv_fallback_cache_dir)
            if csv_fallback_cache_dir
            else _default_csv_cache_dir(db_path)
        )
        fallback = FallbackConfig(
            load_target=db_path,
            cache_dir=cache_dir,
            cli_path=cli_path,
            export_profile=csv_fallback_profile,
            export_format=export_format,
            headless=headless,
            overwrite=overwrite,
            warn=csv_fallback_warn,
        )
        return cls(HybridBackend(derby, fallback))

    @classmethod
    def from_seospider(
        cls,
        crawl_path: str,
        export_dir: str | None = None,
        *,
        backend: str = "derby",
        project_root: str | None = None,
        dbseospider_path: str | None = None,
        materialize_dbseospider: bool = True,
        dbseospider_overwrite: bool = True,
        ensure_db_mode: bool = True,
        spider_config_path: str | None = None,
        cli_path: str | None = None,
        export_tabs: Sequence[str] | None = None,
        bulk_exports: Sequence[str] | None = None,
        save_reports: Sequence[str] | None = None,
        export_format: str = "csv",
        headless: bool = True,
        overwrite: bool = True,
        force_export: bool = False,
        export_profile: str | None = None,
        mapping_path: str | None = None,
        derby_jar: str | None = None,
        csv_fallback: bool = True,
        csv_fallback_cache_dir: str | None = None,
        csv_fallback_profile: str = "kitchen_sink",
        csv_fallback_warn: bool = True,
    ) -> "Crawl":
        mode = backend.strip().lower()
        if mode in {"csv", "exports"}:
            return cls(
                CLIExportBackend(
                    crawl_path,
                    export_dir=export_dir,
                    cli_path=cli_path,
                    export_tabs=export_tabs,
                    bulk_exports=bulk_exports,
                    save_reports=save_reports,
                    export_format=export_format,
                    headless=headless,
                    overwrite=overwrite,
                    force_export=force_export,
                    export_profile=export_profile,
                )
            )

        dbseospider_target = None
        if materialize_dbseospider:
            target = Path(dbseospider_path) if dbseospider_path else Path(crawl_path).with_suffix(".dbseospider")
            dbseospider_target = target
            if target.exists() and not dbseospider_overwrite:
                import warnings

                warnings.warn(
                    f"Using existing .dbseospider cache at {target}. "
                    "Set dbseospider_overwrite=True to refresh. "
                    "Set materialize_dbseospider=False to avoid extra disk usage.",
                    RuntimeWarning,
                )
                return cls.from_derby(str(target), mapping_path=mapping_path, derby_jar=derby_jar)

        project_dir = load_seospider_db_project(
            crawl_path,
            project_root=project_root,
            spider_config_path=spider_config_path,
            cli_path=cli_path,
            export_dir=export_dir,
            export_tabs=export_tabs,
            bulk_exports=bulk_exports,
            save_reports=save_reports,
            export_format=export_format,
            export_profile=export_profile,
            headless=headless,
            overwrite=overwrite,
            ensure_db_mode=ensure_db_mode,
        )

        if materialize_dbseospider:
            import warnings

            if dbseospider_target is None:
                dbseospider_target = Path(dbseospider_path) if dbseospider_path else Path(crawl_path).with_suffix(".dbseospider")
            if dbseospider_target.exists() and dbseospider_overwrite:
                warnings.warn(
                    f"Overwriting existing .dbseospider at {dbseospider_target}. "
                    "Set dbseospider_overwrite=False to keep the existing file. "
                    "Set materialize_dbseospider=False to avoid extra disk usage.",
                    RuntimeWarning,
                )
            else:
                warnings.warn(
                    f"Materializing .dbseospider cache at {dbseospider_target}. "
                    "Set materialize_dbseospider=False to avoid extra disk usage.",
                    RuntimeWarning,
                )

            dbseospider = pack_dbseospider(project_dir, dbseospider_target)
            return cls.from_derby(
                str(dbseospider),
                mapping_path=mapping_path,
                derby_jar=derby_jar,
                csv_fallback=csv_fallback,
                csv_fallback_cache_dir=csv_fallback_cache_dir,
                csv_fallback_profile=csv_fallback_profile,
                csv_fallback_warn=csv_fallback_warn,
                cli_path=cli_path,
                export_format=export_format,
                headless=headless,
                overwrite=overwrite,
            )

        return cls.from_derby(
            str(project_dir),
            mapping_path=mapping_path,
            derby_jar=derby_jar,
            csv_fallback=csv_fallback,
            csv_fallback_cache_dir=csv_fallback_cache_dir,
            csv_fallback_profile=csv_fallback_profile,
            csv_fallback_warn=csv_fallback_warn,
            cli_path=cli_path,
            export_format=export_format,
            headless=headless,
            overwrite=overwrite,
        )

    @classmethod
    def from_db_id(
        cls,
        crawl_id: str,
        export_dir: str | None = None,
        *,
        backend: str = "derby",
        project_root: str | None = None,
        cli_path: str | None = None,
        export_tabs: Sequence[str] | None = None,
        bulk_exports: Sequence[str] | None = None,
        save_reports: Sequence[str] | None = None,
        export_format: str = "csv",
        headless: bool = True,
        overwrite: bool = True,
        force_export: bool = False,
        export_profile: str | None = None,
        mapping_path: str | None = None,
        derby_jar: str | None = None,
        csv_fallback: bool = True,
        csv_fallback_cache_dir: str | None = None,
        csv_fallback_profile: str = "kitchen_sink",
        csv_fallback_warn: bool = True,
    ) -> "Crawl":
        mode = backend.strip().lower()
        if mode in {"csv", "exports"}:
            return cls(
                CLIExportBackend(
                    crawl_id,
                    export_dir=export_dir,
                    cli_path=cli_path,
                    export_tabs=export_tabs,
                    bulk_exports=bulk_exports,
                    save_reports=save_reports,
                    export_format=export_format,
                    headless=headless,
                    overwrite=overwrite,
                    force_export=force_export,
                    export_profile=export_profile,
                )
            )

        project_dir = find_project_dir(crawl_id, project_root=project_root)
        return cls.from_derby(
            str(project_dir),
            mapping_path=mapping_path,
            derby_jar=derby_jar,
            csv_fallback=csv_fallback,
            csv_fallback_cache_dir=csv_fallback_cache_dir,
            csv_fallback_profile=csv_fallback_profile,
            csv_fallback_warn=csv_fallback_warn,
            cli_path=cli_path,
            export_format=export_format,
            headless=headless,
            overwrite=overwrite,
        )

    @classmethod
    def load(
        cls,
        path: str,
        *,
        source_type: str = "auto",
        export_dir: str | None = None,
        cli_path: str | None = None,
        export_tabs: Sequence[str] | None = None,
        bulk_exports: Sequence[str] | None = None,
        save_reports: Sequence[str] | None = None,
        export_format: str = "csv",
        headless: bool = True,
        overwrite: bool = True,
        force_export: bool = False,
        export_profile: str | None = None,
        mapping_path: str | None = None,
        derby_jar: str | None = None,
        seospider_backend: str = "derby",
        db_id_backend: str = "derby",
        project_root: str | None = None,
        dbseospider_path: str | None = None,
        materialize_dbseospider: bool = True,
        dbseospider_overwrite: bool = True,
        ensure_db_mode: bool = True,
        spider_config_path: str | None = None,
        csv_fallback: bool = True,
        csv_fallback_cache_dir: str | None = None,
        csv_fallback_profile: str = "kitchen_sink",
        csv_fallback_warn: bool = True,
    ) -> "Crawl":
        if source_type != "auto":
            return cls._load_by_type(
                path,
                source_type=source_type,
                export_dir=export_dir,
                cli_path=cli_path,
                export_tabs=export_tabs,
                bulk_exports=bulk_exports,
                save_reports=save_reports,
                export_format=export_format,
                headless=headless,
                overwrite=overwrite,
                force_export=force_export,
                export_profile=export_profile,
                mapping_path=mapping_path,
                derby_jar=derby_jar,
                seospider_backend=seospider_backend,
                db_id_backend=db_id_backend,
                project_root=project_root,
                dbseospider_path=dbseospider_path,
                materialize_dbseospider=materialize_dbseospider,
                dbseospider_overwrite=dbseospider_overwrite,
                ensure_db_mode=ensure_db_mode,
                spider_config_path=spider_config_path,
                csv_fallback=csv_fallback,
                csv_fallback_cache_dir=csv_fallback_cache_dir,
                csv_fallback_profile=csv_fallback_profile,
                csv_fallback_warn=csv_fallback_warn,
            )

        path_obj = Path(path)
        if path_obj.is_dir():
            if _looks_like_export_dir(path_obj):
                return cls.from_exports(str(path_obj))
            if _looks_like_derby_dir(path_obj):
                return cls.from_derby(str(path_obj), mapping_path=mapping_path, derby_jar=derby_jar)
        if path_obj.is_file():
            suffix = path_obj.suffix.lower()
            if suffix in {".sqlite", ".db"}:
                return cls.from_database(str(path_obj))
            if suffix == ".dbseospider":
                if _looks_like_sqlite(path_obj):
                    return cls.from_database(str(path_obj))
                return cls.from_derby(
                    str(path_obj),
                    mapping_path=mapping_path,
                    derby_jar=derby_jar,
                    csv_fallback=csv_fallback,
                    csv_fallback_cache_dir=csv_fallback_cache_dir,
                    csv_fallback_profile=csv_fallback_profile,
                    csv_fallback_warn=csv_fallback_warn,
                    cli_path=cli_path,
                    export_format=export_format,
                    headless=headless,
                    overwrite=overwrite,
                )
            if suffix == ".seospider":
                return cls.from_seospider(
                    str(path_obj),
                    export_dir=export_dir,
                    backend=seospider_backend,
                    project_root=project_root,
                    dbseospider_path=dbseospider_path,
                    materialize_dbseospider=materialize_dbseospider,
                    dbseospider_overwrite=dbseospider_overwrite,
                    ensure_db_mode=ensure_db_mode,
                    spider_config_path=spider_config_path,
                    cli_path=cli_path,
                    export_tabs=export_tabs,
                    bulk_exports=bulk_exports,
                    save_reports=save_reports,
                    export_format=export_format,
                    headless=headless,
                    overwrite=overwrite,
                    force_export=force_export,
                    export_profile=export_profile,
                    mapping_path=mapping_path,
                    derby_jar=derby_jar,
                    csv_fallback=csv_fallback,
                    csv_fallback_cache_dir=csv_fallback_cache_dir,
                    csv_fallback_profile=csv_fallback_profile,
                    csv_fallback_warn=csv_fallback_warn,
                )

        if not path_obj.exists() and _looks_like_db_id(path):
            return cls.from_db_id(
                path,
                export_dir=export_dir,
                backend=db_id_backend,
                project_root=project_root,
                cli_path=cli_path,
                export_tabs=export_tabs,
                bulk_exports=bulk_exports,
                save_reports=save_reports,
                export_format=export_format,
                headless=headless,
                overwrite=overwrite,
                force_export=force_export,
                export_profile=export_profile,
                mapping_path=mapping_path,
                derby_jar=derby_jar,
                csv_fallback=csv_fallback,
                csv_fallback_cache_dir=csv_fallback_cache_dir,
                csv_fallback_profile=csv_fallback_profile,
                csv_fallback_warn=csv_fallback_warn,
            )

        raise ValueError(
            f"Unable to detect crawl backend for source: {path}. "
            "Provide source_type to force a loader."
        )

    def tab(self, name: str) -> TabView:
        """Access a generic export tab by CSV name (e.g. 'internal_all.csv')."""
        return TabView(self._backend, name)

    def inlinks(self, url: str) -> Iterator["Link"]:
        """Return inlinks for a given URL (backend-dependent support)."""
        return self._backend.get_inlinks(url)

    def outlinks(self, url: str) -> Iterator["Link"]:
        """Return outlinks for a given URL (backend-dependent support)."""
        return self._backend.get_outlinks(url)

    def redirect_chains(
        self,
        *,
        min_hops: int | None = None,
        max_hops: int | None = None,
        loop: bool | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Return redirect chain rows with optional hop/loop filtering."""
        return self._iter_chain_tab(
            "redirect_chains",
            hop_column="Number of Redirects",
            min_hops=min_hops,
            max_hops=max_hops,
            loop=loop,
        )

    def canonical_chains(
        self,
        *,
        min_hops: int | None = None,
        max_hops: int | None = None,
        loop: bool | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Return canonical chain rows with optional hop/loop filtering."""
        return self._iter_chain_tab(
            "canonical_chains",
            hop_column="Number of Canonicals",
            min_hops=min_hops,
            max_hops=max_hops,
            loop=loop,
        )

    def redirect_and_canonical_chains(
        self,
        *,
        min_hops: int | None = None,
        max_hops: int | None = None,
        loop: bool | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Return mixed redirect/canonical chain rows with optional hop/loop filtering."""
        return self._iter_chain_tab(
            "redirect_and_canonical_chains",
            hop_column="Number of Redirects/Canonicals",
            min_hops=min_hops,
            max_hops=max_hops,
            loop=loop,
        )

    def tab_filters(self, name: str) -> list[str]:
        """List available GUI filter names for a tab."""
        return [filt.name for filt in list_gui_filters(name)]

    def tab_filter_defs(self, name: str) -> list[Any]:
        """Return filter definitions for a tab."""
        return list(list_gui_filters(name))

    def tab_columns(self, name: str) -> list[str]:
        """Return column names for a tab, when available."""
        backend = self._backend
        if isinstance(backend, CLIExportBackend):
            csv_backend = backend._csv
        elif isinstance(backend, CSVBackend):
            csv_backend = backend
        else:
            csv_backend = None

        if csv_backend is not None:
            try:
                csv_path = csv_backend._resolve_tab_file(name)
            except FileNotFoundError:
                return []
            import csv

            with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.DictReader(handle)
                return list(reader.fieldnames or [])

        if isinstance(backend, DerbyBackend):
            mapping = getattr(backend, "_mapping", {})
            key = normalize_name(name)
            if key and not key.lower().endswith(".csv"):
                key = f"{key}.csv"
            entries = mapping.get(key, [])
            if not entries and key:
                alt = f"{key.removesuffix('.csv')}_all.csv"
                entries = mapping.get(alt, [])
            return [entry.get("csv_column") for entry in entries if entry.get("csv_column")]

        if isinstance(backend, DatabaseBackend):
            try:
                return backend.tab_columns(name)
            except Exception:
                return []

        return []

    def describe_tab(self, name: str) -> dict[str, Any]:
        """Return basic metadata for a tab (columns + GUI filters)."""
        return {
            "tab": name,
            "columns": self.tab_columns(name),
            "filters": self.tab_filters(name),
        }

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        """Return raw rows from a backend table (DB-backed only)."""
        return self._backend.raw(table)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        """Execute SQL and yield rows as dicts (DB-backed only)."""
        return self._backend.sql(query, params=params)

    def compare(
        self,
        other: "Crawl",
        *,
        title_fields: Optional[Sequence[str]] = None,
        redirect_fields: Optional[Sequence[str]] = None,
        redirect_type_fields: Optional[Sequence[str]] = None,
        field_groups: Optional[dict[str, Sequence[str]]] = None,
    ) -> CrawlDiff:
        """Compare two crawls and return structural changes."""
        title_fields = title_fields or ("Title 1", "Title")
        redirect_fields = redirect_fields or (
            "Redirect URL",
            "Redirect URI",
            "Redirect Destination",
        )
        redirect_type_fields = redirect_type_fields or ("Redirect Type",)
        field_groups = _DEFAULT_FIELD_GROUPS if field_groups is None else field_groups

        new_pages = _index_internal(self)
        old_pages = _index_internal(other)
        new_pages_norm = _index_internal_normalized(self)
        old_pages_norm = _index_internal_normalized(other)

        new_urls = set(new_pages.keys())
        old_urls = set(old_pages.keys())

        added = sorted(new_urls - old_urls)
        removed = sorted(old_urls - new_urls)

        status_changes: list[StatusChange] = []
        title_changes: list[TitleChange] = []
        redirect_changes: list[RedirectChange] = []
        field_changes: list[FieldChange] = []

        for url in sorted(new_urls & old_urls):
            new_page = new_pages[url]
            old_page = old_pages[url]

            if new_page.status_code != old_page.status_code:
                status_changes.append(
                    StatusChange(
                        url=url,
                        old_status=old_page.status_code,
                        new_status=new_page.status_code,
                    )
                )

            new_title = _get_first_value(new_page.data, title_fields)
            old_title = _get_first_value(old_page.data, title_fields)
            if _diff_values(old_title, new_title):
                title_changes.append(
                    TitleChange(url=url, old_title=old_title, new_title=new_title)
                )

            new_redirect, new_rtype = _resolve_redirect(new_page, redirect_fields, redirect_type_fields)
            old_redirect, old_rtype = _resolve_redirect(old_page, redirect_fields, redirect_type_fields)
            if _diff_values(old_redirect, new_redirect) or _diff_values(old_rtype, new_rtype):
                if old_redirect is not None or new_redirect is not None:
                    redirect_changes.append(
                        RedirectChange(
                            url=url,
                            old_target=old_redirect,
                            new_target=new_redirect,
                            old_type=old_rtype,
                            new_type=new_rtype,
                        )
                    )

            for label, candidates in field_groups.items():
                if label == "Directives Summary":
                    new_value = _directives_summary(new_page)
                    old_value = _directives_summary(old_page)
                elif label == "Canonical Status":
                    canonical_fields = candidates or _DEFAULT_FIELD_GROUPS.get("Canonical", ())
                    new_value = _canonical_status(new_page, new_pages, new_pages_norm, canonical_fields)
                    old_value = _canonical_status(old_page, old_pages, old_pages_norm, canonical_fields)
                else:
                    new_value = _get_first_value(new_page.data, candidates)
                    old_value = _get_first_value(old_page.data, candidates)
                if _diff_values(old_value, new_value):
                    field_changes.append(
                        FieldChange(
                            url=url,
                            field=label,
                            old_value=old_value,
                            new_value=new_value,
                        )
                    )

        return CrawlDiff(
            added_pages=added,
            removed_pages=removed,
            status_changes=status_changes,
            title_changes=title_changes,
            redirect_changes=redirect_changes,
            field_changes=field_changes,
        )

    def _iter_chain_tab(
        self,
        tab_name: str,
        *,
        hop_column: str,
        min_hops: int | None,
        max_hops: int | None,
        loop: bool | None,
    ) -> Iterator[dict[str, Any]]:
        if min_hops is not None and min_hops < 0:
            raise ValueError("min_hops must be >= 0")
        if max_hops is not None and max_hops < 0:
            raise ValueError("max_hops must be >= 0")
        if min_hops is not None and max_hops is not None and min_hops > max_hops:
            raise ValueError("min_hops cannot be greater than max_hops")

        for row in self.tab(tab_name):
            hops = _safe_int(row.get(hop_column))
            if min_hops is not None and (hops is None or hops < min_hops):
                continue
            if max_hops is not None and (hops is None or hops > max_hops):
                continue
            if loop is not None:
                loop_value = _to_bool(row.get("Loop"))
                if loop_value is None or loop_value != loop:
                    continue
            yield row

    @property
    def tabs(self) -> list[str]:
        """List available export tabs for the current backend."""
        try:
            return self._backend.list_tabs()
        except NotImplementedError:
            return []

    @classmethod
    def _load_by_type(
        cls,
        path: str,
        *,
        source_type: str,
        export_dir: str | None,
        cli_path: str | None,
        export_tabs: Sequence[str] | None,
        bulk_exports: Sequence[str] | None,
        save_reports: Sequence[str] | None,
        export_format: str,
        headless: bool,
        overwrite: bool,
        force_export: bool,
        export_profile: str | None,
        mapping_path: str | None,
        derby_jar: str | None,
        seospider_backend: str,
        db_id_backend: str,
        project_root: str | None,
        dbseospider_path: str | None,
        materialize_dbseospider: bool,
        dbseospider_overwrite: bool,
        ensure_db_mode: bool,
        spider_config_path: str | None,
        csv_fallback: bool,
        csv_fallback_cache_dir: str | None,
        csv_fallback_profile: str,
        csv_fallback_warn: bool,
    ) -> "Crawl":
        normalized = source_type.strip().lower()
        if normalized in {"exports", "csv"}:
            return cls.from_exports(path)
        if normalized in {"sqlite", "db"}:
            return cls.from_database(path)
        if normalized in {"derby", "dbseospider"}:
            return cls.from_derby(
                path,
                mapping_path=mapping_path,
                derby_jar=derby_jar,
                csv_fallback=csv_fallback,
                csv_fallback_cache_dir=csv_fallback_cache_dir,
                csv_fallback_profile=csv_fallback_profile,
                csv_fallback_warn=csv_fallback_warn,
                cli_path=cli_path,
                export_format=export_format,
                headless=headless,
                overwrite=overwrite,
            )
        if normalized in {"seospider"}:
            return cls.from_seospider(
                path,
                export_dir=export_dir,
                backend=seospider_backend,
                project_root=project_root,
                dbseospider_path=dbseospider_path,
                materialize_dbseospider=materialize_dbseospider,
                dbseospider_overwrite=dbseospider_overwrite,
                ensure_db_mode=ensure_db_mode,
                spider_config_path=spider_config_path,
                cli_path=cli_path,
                export_tabs=export_tabs,
                bulk_exports=bulk_exports,
                save_reports=save_reports,
                export_format=export_format,
                headless=headless,
                overwrite=overwrite,
                force_export=force_export,
                export_profile=export_profile,
                mapping_path=mapping_path,
                derby_jar=derby_jar,
                csv_fallback=csv_fallback,
                csv_fallback_cache_dir=csv_fallback_cache_dir,
                csv_fallback_profile=csv_fallback_profile,
                csv_fallback_warn=csv_fallback_warn,
            )
        if normalized in {"db_id", "database_id", "dbid"}:
            return cls.from_db_id(
                path,
                export_dir=export_dir,
                backend=db_id_backend,
                project_root=project_root,
                cli_path=cli_path,
                export_tabs=export_tabs,
                bulk_exports=bulk_exports,
                save_reports=save_reports,
                export_format=export_format,
                headless=headless,
                overwrite=overwrite,
                force_export=force_export,
                export_profile=export_profile,
                mapping_path=mapping_path,
                derby_jar=derby_jar,
                csv_fallback=csv_fallback,
                csv_fallback_cache_dir=csv_fallback_cache_dir,
                csv_fallback_profile=csv_fallback_profile,
                csv_fallback_warn=csv_fallback_warn,
            )
        raise ValueError(f"Unknown source_type: {source_type}")


def _index_internal(crawl: Crawl) -> dict[str, InternalPage]:
    pages: dict[str, InternalPage] = {}
    for page in crawl.internal:
        if page.address:
            pages[page.address] = page
    return pages


def _index_internal_normalized(crawl: Crawl) -> dict[str, InternalPage]:
    pages: dict[str, InternalPage] = {}
    for page in crawl.internal:
        if not page.address:
            continue
        key = _normalize_url_for_compare(page.address)
        if key:
            pages[key] = page
    return pages


def _get_first_value(data: dict[str, Any], keys: Sequence[str]) -> Optional[str]:
    normalized = {str(k).strip().lower(): v for k, v in data.items()}
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return str(data[key])
        lower = key.strip().lower()
        if lower in normalized and normalized[lower] not in (None, ""):
            return str(normalized[lower])
    return None


def _diff_values(old: Optional[str], new: Optional[str]) -> bool:
    if old is None and new is None:
        return False
    return str(old) != str(new)


def _normalize_url_for_compare(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text)
    except Exception:
        return text
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or ""
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    return urlunsplit((scheme, netloc, path, parts.query, ""))


def _parse_directives(value: Optional[str]) -> set[str]:
    if value is None:
        return set()
    text = str(value)
    if not text.strip():
        return set()
    parts = re.split(r"[;,\\s]+", text)
    return {part.strip().lower() for part in parts if part.strip()}


def _directives_summary(page: InternalPage) -> Optional[str]:
    meta = _get_first_value(
        page.data,
        _DEFAULT_FIELD_GROUPS.get("Meta Robots", ("Meta Robots 1", "Meta Robots")),
    )
    xrobots = _get_first_value(
        page.data,
        _DEFAULT_FIELD_GROUPS.get("X-Robots-Tag", ("X-Robots-Tag 1", "X-Robots-Tag")),
    )
    tokens = _parse_directives(meta) | _parse_directives(xrobots)
    if not tokens:
        return None
    return ",".join(sorted(tokens))


def _is_non_indexable(page: InternalPage) -> bool:
    idx = _get_first_value(
        page.data, ("Indexability", "Indexability Status", "INDEXABILITY", "INDEXABILITY_STATUS")
    )
    if idx is not None:
        text = str(idx).lower()
        if "non-indexable" in text or "noindex" in text:
            return True
    directives = _directives_summary(page) or ""
    return "noindex" in directives.split(",")


def _has_http_header_canonical(page: InternalPage) -> bool:
    canonical = _get_first_value(page.data, ("HTTP Canonical", "HTTP_CANONICAL"))
    if canonical:
        return True
    headers_blob = page.data.get("HTTP_RESPONSE_HEADER_COLLECTION") or page.data.get(
        "http_response_header_collection"
    )
    if not headers_blob:
        return False
    try:
        from screamingfrog.backends.derby_backend import (  # type: ignore
            _extract_link_rel,
            _headers_from_blob,
            _parse_link_headers,
        )
    except Exception:
        return False
    headers = _headers_from_blob(headers_blob)
    if not headers:
        return False
    links = _parse_link_headers(headers.get("link", []))
    if not links:
        return False
    return _extract_link_rel(links, "canonical") is not None


def _canonical_status(
    page: InternalPage,
    pages_by_url: dict[str, InternalPage],
    pages_by_norm: dict[str, InternalPage],
    canonical_fields: Sequence[str],
) -> Optional[str]:
    canonical = _get_first_value(page.data, canonical_fields)
    if not canonical:
        return None
    canonical_resolved = urljoin(page.address, canonical) if page.address else canonical
    flags: list[str] = []
    page_norm = _normalize_url_for_compare(page.address) if page.address else ""
    canonical_norm = _normalize_url_for_compare(canonical_resolved)
    if page_norm and canonical_norm and page_norm == canonical_norm:
        flags.append("self-ref")
    target = pages_by_url.get(canonical_resolved) or pages_by_norm.get(canonical_norm)
    if target and _is_non_indexable(target):
        flags.append("non-indexable")
    if _has_http_header_canonical(page):
        flags.append("http-header")
    if not flags:
        return None
    return ",".join(flags)


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.strip() == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _resolve_redirect(
    page: InternalPage,
    redirect_fields: Sequence[str],
    redirect_type_fields: Sequence[str],
) -> tuple[Optional[str], Optional[str]]:
    data = page.data
    target = _get_first_value(data, redirect_fields)
    rtype = _get_first_value(data, redirect_type_fields)
    if target:
        return target, rtype

    code = _safe_int(_get_first_value(data, ("Status Code", "RESPONSE_CODE")))
    num_meta = _safe_int(_get_first_value(data, ("NUM_METAREFRESH", "num_metarefresh")))
    meta_url = _get_first_value(data, ("META_FULL_URL_1", "META_FULL_URL_2"))

    if num_meta and meta_url:
        return urljoin(page.address, meta_url), "Meta Refresh"

    headers_blob = data.get("HTTP_RESPONSE_HEADER_COLLECTION") or data.get(
        "http_response_header_collection"
    )
    if code and 300 <= code < 400 and headers_blob is not None:
        try:
            from screamingfrog.backends.derby_backend import _headers_from_blob  # type: ignore
        except Exception:
            _headers_from_blob = None
        if _headers_from_blob:
            headers = _headers_from_blob(headers_blob)
            locations = headers.get("location", [])
            if locations:
                return urljoin(page.address, locations[0]), "HTTP Redirect"

    return None, None


def _looks_like_sqlite(path: Path) -> bool:
    try:
        with path.open("rb") as handle:
            header = handle.read(16)
        return header.startswith(b"SQLite format 3")
    except OSError:
        return False


def _looks_like_export_dir(path: Path) -> bool:
    return any(path.glob("*.csv"))


def _looks_like_derby_dir(path: Path) -> bool:
    if (path / "service.properties").exists():
        return True
    return find_derby_db_root(path) is not None


def _looks_like_db_id(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    # Typical DB IDs are UUIDs.
    if len(stripped) in {32, 36} and all(ch in "0123456789abcdefABCDEF-" for ch in stripped):
        return stripped.count("-") in {0, 4}
    return False


def _default_csv_cache_dir(source: str) -> Path:
    path_obj = Path(source)
    if path_obj.exists():
        if path_obj.is_dir():
            return path_obj / "exports_cache"
        stem = path_obj.stem
        return path_obj.with_name(f"{stem}_exports_cache")
    try:
        project_dir = find_project_dir(source)
        return project_dir / "exports_cache"
    except Exception:
        return Path.cwd() / f"{source}_exports_cache"
