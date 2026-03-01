from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


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
