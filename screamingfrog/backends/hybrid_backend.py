from __future__ import annotations

import json
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from screamingfrog.backends.base import CrawlBackend
from screamingfrog.backends.csv_backend import CSVBackend
from screamingfrog.backends.derby_backend import DerbyBackend
from screamingfrog.cli.exports import export_crawl
from screamingfrog.config import get_export_profile
from screamingfrog.filters.names import make_tab_filename, normalize_name
from screamingfrog.filters.registry import get_filter


@dataclass
class FallbackConfig:
    load_target: str
    cache_dir: Path
    cli_path: Optional[str] = None
    export_profile: str = "kitchen_sink"
    export_format: str = "csv"
    headless: bool = True
    overwrite: bool = False
    warn: bool = True


class HybridBackend(CrawlBackend):
    """Derby-first backend with CSV fallback for GUI parity."""

    def __init__(
        self,
        primary: DerbyBackend,
        fallback: Optional[FallbackConfig] = None,
    ) -> None:
        self._primary = primary
        self._fallback = fallback
        self._schema_dir = _resolve_schema_dir()
        self._warned: set[str] = set()

    def get_internal(self, filters: Optional[dict[str, Any]] = None):
        return self._primary.get_internal(filters=filters)

    def get_inlinks(self, url: str):
        return self._primary.get_inlinks(url)

    def get_outlinks(self, url: str):
        return self._primary.get_outlinks(url)

    def count(self, table: str, filters: Optional[dict[str, Any]] = None) -> int:
        return self._primary.count(table, filters=filters)

    def aggregate(self, table: str, column: str, func: str) -> Any:
        return self._primary.aggregate(table, column, func)

    def list_tabs(self) -> list[str]:
        return self._primary.list_tabs()

    def raw(self, table: str) -> Iterable[dict[str, Any]]:
        return self._primary.raw(table)

    def sql(self, query: str, params: Optional[Sequence[Any]] = None) -> Iterable[dict[str, Any]]:
        return self._primary.sql(query, params=params)

    def close(self) -> None:
        primary = getattr(self, "_primary", None)
        if primary is None:
            return
        close = getattr(primary, "close", None)
        if callable(close):
            close()

    def get_tab(
        self, tab_name: str, filters: Optional[dict[str, Any]] = None
    ) -> Iterable[dict[str, Any]]:
        filters = dict(filters or {})
        gui_filter = filters.get("__gui__")
        if self._should_fallback(tab_name, gui_filter):
            return self._fallback_tab(tab_name, filters)
        return self._primary.get_tab(tab_name, filters=filters)

    def _should_fallback(self, tab_name: str, gui_filter: Any) -> bool:
        if not self._fallback:
            return False
        if gui_filter and isinstance(gui_filter, (list, tuple, set)):
            if len(gui_filter) > 1:
                return False
        if gui_filter:
            if not _gui_filter_supported(tab_name, gui_filter):
                return True
        return _mapping_missing_columns(self._primary, tab_name, gui_filter, self._schema_dir)

    def _fallback_tab(
        self, tab_name: str, filters: dict[str, Any]
    ) -> Iterable[dict[str, Any]]:
        if not self._fallback:
            raise RuntimeError("CSV fallback requested but no fallback configuration provided.")

        gui_filter = filters.get("__gui__")
        if gui_filter and isinstance(gui_filter, (list, tuple, set)):
            gui_filter = list(gui_filter)[0]

        export_label = _resolve_export_label(
            tab_name,
            gui_filter,
            export_profile=self._fallback.export_profile,
        )
        expected_file = _expected_csv_filename(tab_name, gui_filter)
        export_dir = self._fallback.cache_dir
        export_dir.mkdir(parents=True, exist_ok=True)

        csv_path = export_dir / expected_file
        if not csv_path.exists():
            try:
                export_crawl(
                    self._fallback.load_target,
                    export_dir,
                    cli_path=self._fallback.cli_path,
                    export_tabs=(export_label,),
                    export_format=self._fallback.export_format,
                    headless=self._fallback.headless,
                    overwrite=self._fallback.overwrite,
                    force=True,
                )
            except Exception as exc:  # pragma: no cover - CLI errors
                raise RuntimeError(
                    "CSV fallback required for GUI parity, but Screaming Frog CLI export failed. "
                    "Install Screaming Frog, set SCREAMINGFROG_CLI, or provide CSV exports."
                ) from exc

        if self._fallback.warn:
            key = f"{tab_name}:{gui_filter}" if gui_filter else tab_name
            if key not in self._warned:
                warnings.warn(
                    f"CSV fallback used for '{key}'. CSV exports cached at {export_dir}.",
                    RuntimeWarning,
                )
                self._warned.add(key)

        return CSVBackend(str(export_dir)).get_tab(tab_name, filters=filters)


def _resolve_schema_dir() -> Optional[Path]:
    candidates = [
        Path.cwd() / "schemas" / "csv",
        Path(__file__).resolve().parents[2] / "schemas" / "csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _schema_columns(schema_dir: Optional[Path], csv_key: str) -> list[str]:
    if not schema_dir:
        return []
    schema_path = schema_dir / csv_key.replace(".csv", ".json")
    if not schema_path.exists():
        return []
    try:
        data = json.loads(schema_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return [col.get("name") for col in data.get("columns", []) if col.get("name")]


def _mapping_missing_columns(
    primary: DerbyBackend,
    tab_name: str,
    gui_filter: Any,
    schema_dir: Optional[Path],
) -> bool:
    mapping = getattr(primary, "_mapping", {})
    csv_key = _resolve_csv_key(tab_name, gui_filter, schema_dir)
    if not csv_key:
        return False
    required = _schema_columns(schema_dir, csv_key)
    if not required:
        return False
    entries = mapping.get(csv_key, [])
    if gui_filter and not entries:
        base_key = _resolve_csv_key(tab_name, None, schema_dir)
        if base_key:
            entries = mapping.get(base_key, [])
    mapped = {entry.get("csv_column") for entry in entries if entry.get("csv_column")}
    missing = [col for col in required if col not in mapped]
    return bool(missing)


def _resolve_csv_key(
    tab_name: str, gui_filter: Any, schema_dir: Optional[Path]
) -> Optional[str]:
    if gui_filter:
        return _expected_csv_filename(tab_name, gui_filter)
    base = normalize_name(tab_name)
    if not base:
        return None
    candidate_all = f"{base}_all.csv"
    if schema_dir and (schema_dir / candidate_all.replace(".csv", ".json")).exists():
        return candidate_all
    return f"{base}.csv"


def _expected_csv_filename(tab_name: str, gui_filter: Any) -> str:
    gui = str(gui_filter) if gui_filter else "All"
    return make_tab_filename(tab_name, gui)


def _gui_filter_supported(tab_name: str, gui_filter: Any) -> bool:
    if gui_filter is None:
        return True
    if isinstance(gui_filter, (list, tuple, set)):
        if len(gui_filter) != 1:
            return False
        gui_filter = list(gui_filter)[0]
    name = str(gui_filter)
    if normalize_name(name) == "all":
        return True
    filt = get_filter(tab_name, name)
    if not filt:
        return False
    if filt.sql_where or filt.join_table:
        return True
    return normalize_name(filt.name) == "all"


def _resolve_export_label(
    tab_name: str,
    gui_filter: Any,
    *,
    export_profile: str,
) -> str:
    if ":" in tab_name and not gui_filter:
        return tab_name

    profile = get_export_profile(export_profile)
    target_tab = normalize_name(tab_name)
    target_filter = normalize_name(str(gui_filter)) if gui_filter else None

    best_match = None
    for label in profile.export_tabs:
        parts = label.split(":", 1)
        tab_part = normalize_name(parts[0])
        filter_part = normalize_name(parts[1]) if len(parts) > 1 else None
        if tab_part != target_tab:
            continue
        if target_filter is None:
            if filter_part in {None, "all"}:
                return label
            if best_match is None:
                best_match = label
        else:
            if filter_part == target_filter:
                return label
    if best_match:
        return best_match
    raise RuntimeError(
        f"CSV fallback requires export tab for '{tab_name}'"
        + (f":{gui_filter}" if gui_filter else "")
        + " but no matching export label was found."
    )
