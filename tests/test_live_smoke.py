from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from screamingfrog import Crawl
from screamingfrog.cli.exports import resolve_cli_path, start_crawl
from screamingfrog.cli.storage import ensure_storage_mode
from screamingfrog.db.packaging import resolve_project_root


def _live_smoke_enabled() -> bool:
    return os.environ.get("SCREAMINGFROG_RUN_LIVE_SMOKE") == "1"


def _derby_runtime_available() -> bool:
    try:
        import jaydebeapi  # noqa: F401
        import jpype  # noqa: F401
    except Exception:
        return False
    return True


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


@pytest.mark.skipif(
    not _live_smoke_enabled(),
    reason="Set SCREAMINGFROG_RUN_LIVE_SMOKE=1 to run live SF smoke tests",
)
@pytest.mark.skipif(
    not _derby_runtime_available(),
    reason="Derby Python dependencies not available",
)
def test_live_custom_extraction_multi_row_smoke(tmp_path: Path) -> None:
    local_builder = Path.home() / "sf-config-builder"
    if local_builder.exists():
        sys.path.insert(0, str(local_builder))
    pytest.importorskip("sfconfig")
    from sfconfig import SFConfig

    try:
        resolve_cli_path()
    except Exception as exc:
        pytest.skip(f"Screaming Frog CLI not available: {exc}")

    template = Path.home() / "default.seospiderconfig"
    if not template.exists():
        pytest.skip("default.seospiderconfig not available")

    site_dir = tmp_path / "site"
    site_dir.mkdir()
    (site_dir / "index.html").write_text(
        """<!doctype html><html><body>
<div class="item">Alpha</div>
<div class="item">Beta</div>
<div class="item">Gamma</div>
<div class="item">Delta</div>
<div class="item">Epsilon</div>
<a href="/index.html">Self</a>
</body></html>""",
        encoding="utf-8",
    )

    port = _free_port()
    server = subprocess.Popen(
        [sys.executable, "-m", "http.server", str(port), "--bind", "127.0.0.1"],
        cwd=str(site_dir),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    project_dir: Path | None = None
    try:
        time.sleep(2)
        config = SFConfig.load(str(template))
        config.clear_extractions()
        config.add_extraction("Items", ".item", selector_type="CSS", extract_mode="TEXT")
        config.max_urls = 10
        config.max_depth = 1
        config_path = tmp_path / "live-smoke.seospiderconfig"
        config.save(str(config_path))

        project_root = resolve_project_root()
        before = {p.name for p in project_root.iterdir() if p.is_dir()}

        with ensure_storage_mode("DB"):
            result = start_crawl(
                f"http://127.0.0.1:{port}/index.html",
                tmp_path / "exports",
                config=config_path,
                save_crawl=True,
            )
        assert result.returncode == 0, result.stderr

        new_projects = [
            p for p in project_root.iterdir() if p.is_dir() and p.name not in before
        ]
        assert new_projects, "No DB-mode project folder created by live smoke crawl"
        project_dir = max(new_projects, key=lambda p: p.stat().st_mtime)

        crawl = Crawl.load(str(project_dir))
        row = next(iter(crawl.tab("custom_extraction_all")))
        assert row["Extractor 1 1"] == "Alpha"
        assert row["Extractor 1 2"] == "Beta"
        assert row["Extractor 1 3"] == "Gamma"
        assert row["Extractor 1 4"] == "Delta"
        assert row["Extractor 1 5"] == "Epsilon"
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except Exception:
            server.kill()
        if project_dir and project_dir.exists():
            shutil.rmtree(project_dir, ignore_errors=True)
