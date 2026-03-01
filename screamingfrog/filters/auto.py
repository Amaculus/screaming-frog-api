from __future__ import annotations

from importlib import resources

from screamingfrog.filters.registry import FilterDef, register_filter


def register_kitchen_sink_filters() -> None:
    """Register filters from the bundled kitchen-sink export tabs list."""
    tab_file = resources.files("screamingfrog.config").joinpath(
        "exports_kitchen_sink_tabs.txt"
    )
    if not tab_file.exists():
        return
    for line in tab_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        tab, filt = line.split(":", 1)
        register_filter(
            FilterDef(
                name=filt.strip(),
                tab=tab.strip(),
                description="Auto-generated from kitchen-sink export list.",
            )
        )


register_kitchen_sink_filters()
