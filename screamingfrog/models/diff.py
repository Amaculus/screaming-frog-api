from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class StatusChange:
    url: str
    old_status: Optional[int]
    new_status: Optional[int]


@dataclass(frozen=True)
class TitleChange:
    url: str
    old_title: Optional[str]
    new_title: Optional[str]


@dataclass(frozen=True)
class RedirectChange:
    url: str
    old_target: Optional[str]
    new_target: Optional[str]
    old_type: Optional[str] = None
    new_type: Optional[str] = None


@dataclass(frozen=True)
class FieldChange:
    url: str
    field: str
    old_value: Optional[str]
    new_value: Optional[str]


@dataclass(frozen=True)
class CrawlDiff:
    added_pages: list[str]
    removed_pages: list[str]
    status_changes: list[StatusChange]
    title_changes: list[TitleChange]
    redirect_changes: list[RedirectChange]
    field_changes: list[FieldChange] = field(default_factory=list)

    def summary(self) -> dict[str, int]:
        return {
            "added_pages": len(self.added_pages),
            "removed_pages": len(self.removed_pages),
            "status_changes": len(self.status_changes),
            "title_changes": len(self.title_changes),
            "redirect_changes": len(self.redirect_changes),
            "field_changes": len(self.field_changes),
            "total_changes": (
                len(self.added_pages)
                + len(self.removed_pages)
                + len(self.status_changes)
                + len(self.title_changes)
                + len(self.redirect_changes)
                + len(self.field_changes)
            ),
        }

    def to_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        rows.extend({"change_type": "added_page", "url": url} for url in self.added_pages)
        rows.extend({"change_type": "removed_page", "url": url} for url in self.removed_pages)
        rows.extend(
            {
                "change_type": "status_change",
                "url": change.url,
                "field": "Status Code",
                "old_value": change.old_status,
                "new_value": change.new_status,
            }
            for change in self.status_changes
        )
        rows.extend(
            {
                "change_type": "title_change",
                "url": change.url,
                "field": "Title 1",
                "old_value": change.old_title,
                "new_value": change.new_title,
            }
            for change in self.title_changes
        )
        rows.extend(
            {
                "change_type": "redirect_change",
                "url": change.url,
                "field": "Redirect URL",
                "old_value": change.old_target,
                "new_value": change.new_target,
                "old_type": change.old_type,
                "new_type": change.new_type,
            }
            for change in self.redirect_changes
        )
        rows.extend(
            {
                "change_type": "field_change",
                "url": change.url,
                "field": change.field,
                "old_value": change.old_value,
                "new_value": change.new_value,
            }
            for change in self.field_changes
        )
        return rows

    def to_pandas(self) -> Any:
        return _dataframe_from_rows(self.to_rows(), "pandas")

    def to_polars(self) -> Any:
        return _dataframe_from_rows(self.to_rows(), "polars")


def _dataframe_from_rows(rows: list[dict[str, Any]], module_name: str) -> Any:
    module = _import_optional_module(module_name)
    return module.DataFrame(rows)


def _import_optional_module(module_name: str) -> Any:
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise ImportError(
            f"{module_name} is required for this export. Install it to use to_{module_name}()."
        ) from exc
