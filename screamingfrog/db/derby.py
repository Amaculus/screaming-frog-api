from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path
from typing import Optional


def extract_dbseospider(db_path: Path, out_dir: Path) -> Path:
    """Extract a .dbseospider file and return the extraction root."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(db_path, "r") as archive:
        archive.extractall(out_dir)
    return out_dir


def find_derby_db_root(root: Path) -> Optional[Path]:
    """Locate the Derby database directory by finding service.properties."""
    for path in root.rglob("service.properties"):
        return path.parent
    return None


def resolve_java_executable(java_home: str | None = None) -> Path:
    """Resolve the java executable path."""
    env_home = java_home or os.environ.get("JAVA_HOME")
    if env_home:
        candidate = Path(env_home) / "bin" / "java.exe"
        if candidate.exists():
            return candidate
    java_path = shutil.which("java")
    if java_path:
        return Path(java_path)
    raise RuntimeError("Java runtime not found. Set JAVA_HOME or add java to PATH.")


def ensure_java_home(java_home: str | None = None) -> None:
    """Ensure JAVA_HOME is set to a usable JRE if possible."""
    if java_home:
        os.environ["JAVA_HOME"] = java_home
        return
    if os.environ.get("JAVA_HOME"):
        return
    candidates = [
        r"C:\Program Files (x86)\Screaming Frog SEO Spider\jre",
        r"C:\Program Files\Screaming Frog SEO Spider\jre",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            os.environ["JAVA_HOME"] = candidate
            return


def resolve_derby_jars(derby_jar: str | None = None) -> list[Path]:
    """Resolve Derby jar paths (supports pathsep-separated list)."""
    jar_value = derby_jar or os.environ.get("DERBY_JAR")
    if jar_value:
        parts = [Path(p.strip()) for p in jar_value.split(os.pathsep) if p.strip()]
        jars = [p for p in parts if p.exists()]
        if jars:
            return jars
    bundled = _bundled_derby_jars()
    if bundled:
        return bundled
    default_jars = _default_derby_jars()
    if default_jars:
        return default_jars
    raise RuntimeError("Derby jars not found. Set DERBY_JAR to one or more jar paths.")


def _bundled_derby_jars() -> list[Path]:
    base = Path(__file__).resolve().parents[1] / "vendor" / "derby"
    if not base.exists():
        return []
    jars: list[Path] = []
    jars.extend(sorted(base.glob("derby-*.jar")))
    jars.extend(sorted(base.glob("derbyshared-*.jar")))
    jars.extend(sorted(base.glob("derbytools-*.jar")))
    return jars


def _default_derby_jars() -> list[Path]:
    candidates = [
        Path(r"C:\Program Files (x86)\Screaming Frog SEO Spider\lib"),
        Path(r"C:\Program Files\Screaming Frog SEO Spider\lib"),
    ]
    for base in candidates:
        if not base.exists():
            continue
        jars: list[Path] = []
        jars.extend(sorted(base.glob("derby-*.jar")))
        jars.extend(sorted(base.glob("derbyshared-*.jar")))
        jars.extend(sorted(base.glob("derbytools-*.jar")))
        if jars:
            return jars
    return []
