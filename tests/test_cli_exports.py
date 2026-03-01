from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from screamingfrog.cli import exports


def test_run_cli_prepends_cli_when_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, object] = {}

    def fake_run(cmd: list[str], capture_output: bool, text: bool) -> subprocess.CompletedProcess[str]:
        recorded["cmd"] = cmd
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    monkeypatch.setattr(exports, "resolve_cli_path", lambda cli_path=None: Path("sfcli.exe"))
    monkeypatch.setattr(subprocess, "run", fake_run)

    result = exports.run_cli(["--help"])

    assert result.returncode == 0
    assert recorded["cmd"] == ["sfcli.exe", "--help"]


def test_run_cli_raises_on_non_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(cmd: list[str], capture_output: bool, text: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="bad")

    monkeypatch.setattr(exports, "resolve_cli_path", lambda cli_path=None: Path("sfcli.exe"))
    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(RuntimeError):
        exports.run_cli(["--help"], check=True)


def test_start_crawl_builds_expected_args(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    recorded: dict[str, object] = {}

    def fake_run_cli(args: list[str], *, cli_path: str | None = None, check: bool = True) -> subprocess.CompletedProcess[str]:
        recorded["args"] = args
        recorded["cli_path"] = cli_path
        recorded["check"] = check
        return subprocess.CompletedProcess(args, 0, stdout="ok", stderr="")

    monkeypatch.setattr(exports, "run_cli", fake_run_cli)
    monkeypatch.setattr(exports, "resolve_cli_path", lambda cli_path=None: Path("sfcli.exe"))

    out = tmp_path / "exports"
    result = exports.start_crawl(
        "https://example.com",
        out,
        config=tmp_path / "cfg.seospiderconfig",
        export_tabs=["Internal:All"],
        save_crawl=True,
        task_name="alpha-test",
        project_name="alpha-project",
    )

    assert result.returncode == 0
    assert out.exists()
    assert recorded["args"] == [
        "sfcli.exe",
        "--crawl",
        "https://example.com",
        "--output-folder",
        str(out),
        "--headless",
        "--overwrite",
        "--save-crawl",
        "--export-format",
        "csv",
        "--config",
        str(tmp_path / "cfg.seospiderconfig"),
        "--task-name",
        "alpha-test",
        "--project-name",
        "alpha-project",
        "--export-tabs",
        "Internal:All",
    ]
