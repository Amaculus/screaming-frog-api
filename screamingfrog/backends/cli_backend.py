from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.backends.csv_backend import CSVBackend
from screamingfrog.cli.exports import DEFAULT_EXPORT_TABS, export_crawl
from screamingfrog.models import InternalPage, Link


class CLIExportBackend(CrawlBackend):
    """Backend that loads a crawl via CLI and reads data from CSV exports."""

    def __init__(
        self,
        load_target: str,
        export_dir: Optional[str] = None,
        *,
        cli_path: Optional[str] = None,
        export_tabs: Optional[Sequence[str]] = None,
        bulk_exports: Optional[Sequence[str]] = None,
        save_reports: Optional[Sequence[str]] = None,
        export_format: str = "csv",
        headless: bool = True,
        overwrite: bool = True,
        force_export: bool = False,
        export_profile: Optional[str] = None,
    ):
        if _looks_like_path(load_target) and not Path(load_target).exists():
            raise FileNotFoundError(f"Crawl file not found: {load_target}")

        self.load_target = load_target
        self.export_dir = Path(export_dir) if export_dir else Path(
            tempfile.mkdtemp(prefix="sf_exports_")
        )
        tabs = export_tabs
        if export_profile:
            # Let the profile populate export lists.
            pass
        elif tabs is None:
            tabs = DEFAULT_EXPORT_TABS

        export_crawl(
            load_target,
            self.export_dir,
            cli_path=cli_path,
            export_tabs=tabs,
            bulk_exports=bulk_exports,
            save_reports=save_reports,
            export_format=export_format,
            headless=headless,
            overwrite=overwrite,
            force=force_export,
            export_profile=export_profile,
        )

        self._csv = CSVBackend(str(self.export_dir))

    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        return self._csv.get_internal(filters=filters)

    def get_inlinks(self, url: str) -> Iterator[Link]:
        return self._csv.get_inlinks(url)

    def get_outlinks(self, url: str) -> Iterator[Link]:
        return self._csv.get_outlinks(url)

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return self._csv.count(table, filters=filters)

    def aggregate(self, table: str, column: str, func: str) -> Any:
        return self._csv.aggregate(table, column, func)

    def list_tabs(self) -> list[str]:
        return self._csv.list_tabs()

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        return self._csv.get_tab(tab_name, filters=filters)

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        raise NotImplementedError("Raw access is only available for database backends.")

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        raise NotImplementedError("SQL access is only available for database backends.")


def _looks_like_path(value: str) -> bool:
    if value.endswith((".seospider", ".dbseospider", ".db", ".sqlite")):
        return True
    if os.sep in value:
        return True
    if os.altsep and os.altsep in value:
        return True
    return False
