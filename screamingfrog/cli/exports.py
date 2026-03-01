from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Iterable, Optional, Sequence


DEFAULT_EXPORT_TABS = ("Internal:All",)
_INTERNAL_CSV_CANDIDATES = (
    "internal_all.csv",
    "Internal All.csv",
    "internal.csv",
)


def export_crawl(
    load_target: str,
    export_dir: str | Path | None = None,
    *,
    cli_path: str | None = None,
    export_tabs: Sequence[str] | None = None,
    bulk_exports: Sequence[str] | None = None,
    save_reports: Sequence[str] | None = None,
    export_format: str = "csv",
    headless: bool = True,
    overwrite: bool = True,
    force: bool = False,
    export_profile: str | None = None,
) -> Path:
    """Export crawl data via Screaming Frog CLI using --load-crawl.

    Returns the export directory path.
    """
    export_dir_path = Path(export_dir) if export_dir else Path(
        tempfile.mkdtemp(prefix="sf_exports_")
    )
    export_dir_path.mkdir(parents=True, exist_ok=True)

    if not force and _internal_csv_exists(export_dir_path):
        return export_dir_path

    cli = resolve_cli_path(cli_path)

    if export_profile:
        from screamingfrog.config import get_export_profile

        profile = get_export_profile(export_profile)
        if not export_tabs:
            export_tabs = profile.export_tabs
        if not bulk_exports:
            bulk_exports = profile.bulk_exports

    if export_tabs is None:
        export_tabs = DEFAULT_EXPORT_TABS

    args = [
        str(cli),
        "--load-crawl",
        str(load_target),
        "--output-folder",
        str(export_dir_path),
    ]
    if headless:
        args.append("--headless")
    if export_format:
        args.extend(["--export-format", export_format])
    if overwrite:
        args.append("--overwrite")

    if export_tabs:
        args.extend(["--export-tabs", ",".join(export_tabs)])
    if bulk_exports:
        args.extend(["--bulk-export", ",".join(bulk_exports)])
    if save_reports:
        args.extend(["--save-report", ",".join(save_reports)])

    result = subprocess.run(
        args,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(_format_cli_error(result))

    return export_dir_path


def start_crawl(
    start_url: str,
    output_dir: str | Path,
    *,
    cli_path: str | None = None,
    config: str | Path | None = None,
    auth_config: str | Path | None = None,
    export_tabs: Sequence[str] | None = None,
    bulk_exports: Sequence[str] | None = None,
    save_reports: Sequence[str] | None = None,
    export_format: str = "csv",
    headless: bool = True,
    overwrite: bool = True,
    save_crawl: bool = False,
    timestamped_output: bool = False,
    task_name: str | None = None,
    project_name: str | None = None,
    extra_args: Sequence[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Start a crawl from a URL via Screaming Frog CLI.

    Returns the completed subprocess result.
    """
    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    args = [
        str(resolve_cli_path(cli_path)),
        "--crawl",
        str(start_url),
        "--output-folder",
        str(output_dir_path),
    ]

    if headless:
        args.append("--headless")
    if overwrite:
        args.append("--overwrite")
    if save_crawl:
        args.append("--save-crawl")
    if timestamped_output:
        args.append("--timestamped-output")
    if export_format:
        args.extend(["--export-format", export_format])
    if config:
        args.extend(["--config", str(config)])
    if auth_config:
        args.extend(["--auth-config", str(auth_config)])
    if task_name:
        args.extend(["--task-name", task_name])
    if project_name:
        args.extend(["--project-name", project_name])
    if export_tabs:
        args.extend(["--export-tabs", ",".join(export_tabs)])
    if bulk_exports:
        args.extend(["--bulk-export", ",".join(bulk_exports)])
    if save_reports:
        args.extend(["--save-report", ",".join(save_reports)])
    if extra_args:
        args.extend(extra_args)

    return run_cli(args)


def run_cli(
    args: Sequence[str],
    *,
    cli_path: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run Screaming Frog CLI with arbitrary arguments.

    If ``args`` does not already start with the CLI executable path,
    it will be prepended automatically.
    """
    cmd = list(args)
    if not cmd:
        raise ValueError("args must contain at least one CLI argument")

    cli = str(resolve_cli_path(cli_path))
    first = Path(cmd[0]).name.lower()
    if "screamingfrogseo" not in first and "screamingfrogseospider" not in first:
        cmd.insert(0, cli)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    if check and result.returncode != 0:
        raise RuntimeError(_format_cli_error(result))
    return result


def resolve_cli_path(cli_path: str | None = None) -> Path:
    """Resolve the Screaming Frog CLI executable path."""
    candidates: list[Path] = []

    if cli_path:
        candidates.append(Path(cli_path))

    env_cli = os.environ.get("SCREAMINGFROG_CLI")
    if env_cli:
        candidates.append(Path(env_cli))

    candidates.extend(_default_cli_candidates())

    for candidate in candidates:
        if candidate.exists():
            return candidate

    which_candidates = ["ScreamingFrogSEOSpiderCli.exe", "ScreamingFrogSEOSpiderCli"]
    if not sys.platform.startswith("win"):
        which_candidates.append("screamingfrogseospider")

    for name in which_candidates:
        resolved = shutil.which(name)
        if resolved:
            return Path(resolved)

    raise RuntimeError(
        "Screaming Frog CLI not found. Provide cli_path or set SCREAMINGFROG_CLI."
    )


def _default_cli_candidates() -> list[Path]:
    if sys.platform.startswith("win"):
        return [
            Path(r"C:\Program Files (x86)\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe"),
            Path(r"C:\Program Files\Screaming Frog SEO Spider\ScreamingFrogSEOSpiderCli.exe"),
        ]
    if sys.platform == "darwin":
        return [
            Path(
                "/Applications/Screaming Frog SEO Spider.app/Contents/MacOS/ScreamingFrogSEOSpider"
            )
        ]
    return [Path("/usr/bin/screamingfrogseospider"), Path("/usr/local/bin/screamingfrogseospider")]


def _internal_csv_exists(export_dir: Path) -> bool:
    for name in _INTERNAL_CSV_CANDIDATES:
        if (export_dir / name).exists():
            return True
    for path in export_dir.glob("*.csv"):
        if path.name.lower() in {c.lower() for c in _INTERNAL_CSV_CANDIDATES}:
            return True
    return False


def _format_cli_error(result: subprocess.CompletedProcess[str]) -> str:
    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    parts: list[str] = [f"Screaming Frog CLI failed (exit {result.returncode})."]
    if stdout:
        parts.append("STDOUT:")
        parts.append(stdout[:4000])
    if stderr:
        parts.append("STDERR:")
        parts.append(stderr[:4000])
    return "\n".join(parts)
