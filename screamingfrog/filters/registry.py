from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional


@dataclass(frozen=True)
class FilterDef:
    """Definition of a GUI-style filter for a specific tab."""

    name: str
    tab: str
    description: str = ""
    sql_where: Optional[str] = None
    join_table: Optional[str] = None
    join_on: Optional[str] = None
    join_type: str = "LEFT"
    columns: List[str] = field(default_factory=list)


class FilterRegistry:
    """Registry of filters available for tabs."""

    def __init__(self) -> None:
        self._filters: Dict[str, Dict[str, FilterDef]] = {}

    def register(self, filt: FilterDef) -> None:
        tab_key = _normalize_key(filt.tab)
        name_key = _normalize_key(filt.name)
        self._filters.setdefault(tab_key, {})[name_key] = filt

    def get(self, tab: str, name: str) -> Optional[FilterDef]:
        return self._filters.get(_normalize_key(tab), {}).get(_normalize_key(name))

    def list_tabs(self) -> List[str]:
        return sorted(self._filters.keys())

    def list_filters(self, tab: str) -> List[FilterDef]:
        return sorted(
            self._filters.get(_normalize_key(tab), {}).values(),
            key=lambda f: f.name.lower(),
        )

    def all_filters(self) -> Iterable[FilterDef]:
        for tab_filters in self._filters.values():
            for filt in tab_filters.values():
                yield filt


_REGISTRY = FilterRegistry()
_FILTERS_LOADED = False


def _ensure_filters_loaded() -> None:
    """Lazy-load the filters package to populate the registry."""
    global _FILTERS_LOADED
    if _FILTERS_LOADED:
        return
    # Importing the package registers all filter definitions as a side effect.
    import importlib

    importlib.import_module("screamingfrog.filters")
    _FILTERS_LOADED = True


def register_filter(filt: FilterDef) -> None:
    _REGISTRY.register(filt)


def get_filter(tab: str, name: str) -> Optional[FilterDef]:
    _ensure_filters_loaded()
    return _REGISTRY.get(tab, name)


def list_filters(tab: str) -> List[FilterDef]:
    _ensure_filters_loaded()
    return _REGISTRY.list_filters(tab)


def list_tabs() -> List[str]:
    _ensure_filters_loaded()
    return _REGISTRY.list_tabs()


def all_filters() -> Iterable[FilterDef]:
    _ensure_filters_loaded()
    return _REGISTRY.all_filters()


def _normalize_key(value: str) -> str:
    return value.strip().lower().replace(" ", "_")
