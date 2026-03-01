from __future__ import annotations

import re


def normalize_name(value: str) -> str:
    text = value.strip().lower()
    text = text.replace("&", "and")
    text = text.replace("/", "_")
    text = text.replace(":", "_")
    text = text.replace("<", "")
    text = text.replace(">", "")
    text = text.replace("'", "")
    text = text.replace("\"", "")
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def make_tab_filename(tab: str, gui_filter: str | None = None) -> str:
    base = normalize_name(tab)
    if gui_filter:
        filt = normalize_name(gui_filter)
        name = f"{base}_{filt}.csv"
    else:
        name = f"{base}.csv"
    return name
