from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Sequence

from screamingfrog.models import InternalPage, Link


class CrawlBackend(ABC):
    """Abstract interface for crawl data backends."""

    @abstractmethod
    def get_internal(self, filters: Optional[dict[str, Any]] = None) -> Iterator[InternalPage]:
        """Yield internal pages, optionally filtered."""

    @abstractmethod
    def get_inlinks(self, url: str) -> Iterator[Link]:
        """Get all inlinks to a URL."""

    @abstractmethod
    def get_outlinks(self, url: str) -> Iterator[Link]:
        """Get all outlinks from a URL."""

    @abstractmethod
    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        """Count rows, optionally filtered."""

    @abstractmethod
    def aggregate(self, table: str, column: str, func: str) -> Any:
        """Run aggregation (sum, avg, count, etc.)."""

    def list_tabs(self) -> list[str]:
        """List available export tabs (CSV-backed backends)."""
        raise NotImplementedError("Tab listing not supported by this backend")

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterator[dict[str, Any]]:
        """Iterate rows for an export tab (CSV-backed backends)."""
        raise NotImplementedError("Tab access not supported by this backend")

    def tab_count(self, tab_name: str, filters: Optional[dict[str, Any]] = None) -> int:
        """Count rows for an export tab."""
        raise NotImplementedError("Tab count not supported by this backend")

    def tab_counts(
        self,
        tab_names: Sequence[str],
        filters: Optional[dict[str, Any]] = None,
    ) -> dict[str, int]:
        """Count rows for multiple export tabs."""
        raise NotImplementedError("Batch tab count not supported by this backend")

    def tab_rows(
        self,
        tab_name: str,
        limit: int,
        filters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """Fetch the first N rows for an export tab."""
        raise NotImplementedError("Tab row sampling not supported by this backend")

    def tab_select(
        self,
        tab_name: str,
        columns: Sequence[str],
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate projected rows for an export tab."""
        raise NotImplementedError("Tab projection not supported by this backend")

    def prefers_source_link_reads(self) -> bool:
        """Return true when this backend should remain source-of-truth for link reads."""
        return False

    def raw(self, table: str) -> Iterator[dict[str, Any]]:
        """Iterate raw rows from a backend-specific table."""
        raise NotImplementedError("Raw table access not supported by this backend")

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterator[dict[str, Any]]:
        """Execute SQL and yield rows as dictionaries."""
        raise NotImplementedError("SQL access not supported by this backend")

    def close(self) -> None:
        """Release backend resources."""
        return None
