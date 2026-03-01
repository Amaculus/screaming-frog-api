from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import os


def resolve_spider_config(config_path: str | Path | None = None) -> Path:
    """Resolve the Screaming Frog spider.config path."""
    candidates: list[Path] = []
    if config_path:
        candidates.append(Path(config_path))

    env_path = os.environ.get("SCREAMINGFROG_SPIDER_CONFIG")
    if env_path:
        candidates.append(Path(env_path))

    home = Path.home() / ".ScreamingFrogSEOSpider" / "spider.config"
    candidates.append(home)

    appdata = os.environ.get("APPDATA")
    if appdata:
        candidates.append(Path(appdata) / "ScreamingFrogSEOSpider" / "spider.config")

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


@contextmanager
def ensure_storage_mode(
    mode: str = "DB",
    *,
    config_path: str | Path | None = None,
) -> Path:
    """Temporarily force storage.mode in spider.config."""
    path = resolve_spider_config(config_path)
    existed = path.exists()
    original = path.read_text(encoding="utf-8") if existed else ""
    updated = _set_config_value(original, "storage.mode", mode)
    if updated != original:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(updated, encoding="utf-8")

    try:
        yield path
    finally:
        if existed:
            path.write_text(original, encoding="utf-8")
        else:
            if path.exists():
                path.unlink()


def _set_config_value(text: str, key: str, value: str) -> str:
    lines = text.splitlines(keepends=True)
    key_prefix = f"{key}="
    found = False
    for idx, line in enumerate(lines):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith(key_prefix):
            lines[idx] = f"{key}={value}\n"
            found = True
        elif stripped.split("=", 1)[0] == key:
            lines[idx] = f"{key}={value}\n"
            found = True

    if not found:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = f"{lines[-1]}\n"
        lines.append(f"{key}={value}\n")
    return "".join(lines)
