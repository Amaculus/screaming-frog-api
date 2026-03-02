from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from screamingfrog import Crawl


def _derby_available() -> bool:
    try:
        import jaydebeapi  # noqa: F401
        import jpype  # noqa: F401
    except Exception:
        return False
    try:
        from screamingfrog.db.derby import (
            ensure_java_home,
            resolve_derby_jars,
            resolve_java_executable,
        )

        # Mirror runtime behavior: infer JAVA_HOME from common SF installs.
        ensure_java_home()
        resolve_derby_jars()
        resolve_java_executable()
    except Exception:
        return False
    return True


@pytest.mark.skipif(not _derby_available(), reason="Derby dependencies not available")
def test_load_dbseospider_smoke(tmp_path: Path) -> None:
    source = Path("tmp_db") / "crawl.zip"
    if not source.exists():
        pytest.skip("Sample dbseospider archive not available")
    target = tmp_path / "sample.dbseospider"
    shutil.copyfile(source, target)

    crawl = Crawl.load(str(target), csv_fallback=False)
    # Basic smoke check: we can iterate internal without crashing.
    _ = crawl.internal.count()
