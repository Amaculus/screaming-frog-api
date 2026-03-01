from __future__ import annotations

import os
import shutil
import tempfile
import zipfile
from contextlib import nullcontext
from pathlib import Path
from typing import Iterable, Optional

from screamingfrog.cli.exports import DEFAULT_EXPORT_TABS, export_crawl
from screamingfrog.cli.storage import ensure_storage_mode


def pack_dbseospider(project_dir: str | Path, output_file: str | Path) -> Path:
    """Package a ProjectInstanceData crawl folder into a .dbseospider file."""
    project_path = Path(project_dir)
    if not project_path.exists():
        raise FileNotFoundError(f"Project directory not found: {project_path}")
    if not project_path.is_dir():
        raise ValueError(f"Project path is not a directory: {project_path}")

    output_path = Path(output_file)
    if output_path.suffix.lower() != ".dbseospider":
        output_path = output_path.with_suffix(".dbseospider")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in project_path.rglob("*"):
            if path.is_dir():
                continue
            rel_path = path.relative_to(project_path)
            archive.write(path, rel_path.as_posix())
    return output_path


def unpack_dbseospider(dbseospider_file: str | Path, output_dir: str | Path) -> Path:
    """Extract a .dbseospider file into a directory."""
    input_path = Path(dbseospider_file)
    if not input_path.exists():
        raise FileNotFoundError(f"dbseospider file not found: {input_path}")
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(input_path, "r") as archive:
        archive.extractall(output_path)
    return output_path


def pack_dbseospider_from_db_id(
    db_id: str,
    output_file: str | Path,
    project_root: str | Path | None = None,
) -> Path:
    """Package a DB-mode crawl by Database Id into a .dbseospider file."""
    project_dir = find_project_dir(db_id, project_root=project_root)
    return pack_dbseospider(project_dir, output_file)


def export_dbseospider_from_seospider(
    crawl_path: str | Path,
    output_file: str | Path,
    *,
    project_root: str | Path | None = None,
    spider_config_path: str | Path | None = None,
    cli_path: str | None = None,
    export_dir: str | Path | None = None,
    export_tabs: Optional[Iterable[str]] = None,
    bulk_exports: Optional[Iterable[str]] = None,
    save_reports: Optional[Iterable[str]] = None,
    export_format: str = "csv",
    export_profile: str | None = None,
    headless: bool = True,
    overwrite: bool = True,
    ensure_db_mode: bool = True,
    cleanup_exports: bool = True,
) -> Path:
    """Load a .seospider crawl via CLI and package the resulting DB crawl."""
    project_dir = load_seospider_db_project(
        crawl_path,
        project_root=project_root,
        spider_config_path=spider_config_path,
        cli_path=cli_path,
        export_dir=export_dir,
        export_tabs=export_tabs,
        bulk_exports=bulk_exports,
        save_reports=save_reports,
        export_format=export_format,
        export_profile=export_profile,
        headless=headless,
        overwrite=overwrite,
        ensure_db_mode=ensure_db_mode,
        cleanup_exports=cleanup_exports,
    )
    return pack_dbseospider(project_dir, output_file)


def load_seospider_db_project(
    crawl_path: str | Path,
    *,
    project_root: str | Path | None = None,
    spider_config_path: str | Path | None = None,
    cli_path: str | None = None,
    export_dir: str | Path | None = None,
    export_tabs: Optional[Iterable[str]] = None,
    bulk_exports: Optional[Iterable[str]] = None,
    save_reports: Optional[Iterable[str]] = None,
    export_format: str = "csv",
    export_profile: str | None = None,
    headless: bool = True,
    overwrite: bool = True,
    ensure_db_mode: bool = True,
    cleanup_exports: bool = True,
) -> Path:
    """Load a .seospider crawl via CLI and return the DB crawl directory."""
    crawl_file = Path(crawl_path)
    if not crawl_file.exists():
        raise FileNotFoundError(f"Crawl file not found: {crawl_file}")

    root = resolve_project_root(project_root)
    before = _project_dirs(root)

    ctx = ensure_storage_mode("DB", config_path=spider_config_path) if ensure_db_mode else nullcontext()
    temp_export_dir: Path | None = None
    export_path: str | Path | None = export_dir
    if export_dir is None:
        temp_export_dir = Path(tempfile.mkdtemp(prefix="sf_exports_"))
        export_path = temp_export_dir

    with ctx:
        export_crawl(
            str(crawl_file),
            export_path,
            cli_path=cli_path,
            export_tabs=tuple(export_tabs) if export_tabs is not None else DEFAULT_EXPORT_TABS,
            bulk_exports=tuple(bulk_exports) if bulk_exports else None,
            save_reports=tuple(save_reports) if save_reports else None,
            export_format=export_format,
            export_profile=export_profile,
            headless=headless,
            overwrite=overwrite,
            force=True,
        )

    after = _project_dirs(root)
    new_dirs = [p for p in after if p not in before]
    if not new_dirs:
        raise RuntimeError(
            "No new DB crawl directory detected after loading the .seospider file. "
            "Ensure storage.mode=DB or set ensure_db_mode=True. "
            "Set project_root if your DB path is non-default."
        )

    newest = max(new_dirs, key=lambda p: p.stat().st_mtime)
    if temp_export_dir and cleanup_exports:
        shutil.rmtree(temp_export_dir, ignore_errors=True)
    return newest


def find_project_dir(db_id: str, project_root: str | Path | None = None) -> Path:
    """Locate a ProjectInstanceData directory by Database Id."""
    root = resolve_project_root(project_root)
    candidate = root / db_id
    if candidate.exists() and candidate.is_dir():
        return candidate
    raise FileNotFoundError(f"DB crawl ID not found in {root}: {db_id}")


def resolve_project_root(project_root: str | Path | None = None) -> Path:
    """Resolve the ProjectInstanceData root directory."""
    if project_root:
        root = Path(project_root)
        if root.exists():
            return root
        raise FileNotFoundError(f"Project root not found: {root}")

    env_path = os.environ.get("SCREAMINGFROG_PROJECT_DIR")
    if env_path:
        root = Path(env_path)
        if root.exists():
            return root

    appdata = os.environ.get("APPDATA")
    if appdata:
        root = Path(appdata) / "ScreamingFrogSEOSpider" / "ProjectInstanceData"
        if root.exists():
            return root

    home = Path.home()
    return home / ".ScreamingFrogSEOSpider" / "ProjectInstanceData"


def _project_dirs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    dirs: list[Path] = []
    for path in root.iterdir():
        if not path.is_dir():
            continue
        if (path / "DbSeoSpiderFileKey").exists():
            dirs.append(path)
    return dirs
