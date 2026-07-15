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
    original, encoding = _read_config(path) if existed else ("", "utf-8")
    original_value = _get_config_value(original, "storage.mode")
    updated = _set_config_value(original, "storage.mode", mode)
    if updated != original:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(updated, encoding=encoding)

    try:
        yield path
    finally:
        if path.exists():
            current, current_encoding = _read_config(path)
            restored = (
                _remove_config_value(current, "storage.mode")
                if original_value is None
                else _set_config_value(current, "storage.mode", original_value)
            )
            path.write_text(restored, encoding=current_encoding)
        elif existed:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(original, encoding=encoding)


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
        elif stripped.split("=", 1)[0].strip() == key:
            lines[idx] = f"{key}={value}\n"
            found = True

    if not found:
        if lines and not lines[-1].endswith("\n"):
            lines[-1] = f"{lines[-1]}\n"
        lines.append(f"{key}={value}\n")
    return "".join(lines)


def _read_config(path: Path) -> tuple[str, str]:
    raw = path.read_bytes()
    try:
        return raw.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return raw.decode("cp1252"), "cp1252"


def _get_config_value(text: str, key: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        lhs, value = stripped.split("=", 1)
        if lhs.strip() == key:
            return value.strip()
    return None


def _remove_config_value(text: str, key: str) -> str:
    lines = text.splitlines(keepends=True)
    return "".join(
        line
        for line in lines
        if not (
            line.strip()
            and not line.strip().startswith("#")
            and "=" in line
            and line.strip().split("=", 1)[0].strip() == key
        )
    )
