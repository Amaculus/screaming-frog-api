from __future__ import annotations

from pathlib import Path

from screamingfrog import Crawl
import screamingfrog.crawl as crawl_module


def test_from_derby_defaults_to_export_and_load_duckdb(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}
    sentinel = object()
    source = tmp_path / "crawl.dbseospider"
    source.write_text("stub", encoding="utf-8")
    target = tmp_path / "crawl.duckdb"

    def fake_export(path: str, duckdb_path: Path, **kwargs: object) -> Path:
        calls["path"] = path
        calls["duckdb_path"] = duckdb_path
        calls["kwargs"] = kwargs
        return duckdb_path

    def fake_from_duckdb(cls, path: str):  # type: ignore[no-untyped-def]
        calls["loaded"] = path
        return sentinel

    monkeypatch.setattr(crawl_module, "export_duckdb_from_derby", fake_export)
    monkeypatch.setattr(Crawl, "from_duckdb", classmethod(fake_from_duckdb))

    result = Crawl.from_derby(str(source))

    assert result is sentinel
    assert calls["path"] == str(source)
    assert calls["duckdb_path"] == target
    assert calls["loaded"] == str(target)
    assert calls["kwargs"]["if_exists"] == "auto"


def test_from_db_id_can_export_and_load_duckdb(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}
    sentinel = object()
    target = tmp_path / "crawl.duckdb"

    def fake_export(crawl_id: str, path: Path, **kwargs: object) -> Path:
        calls["crawl_id"] = crawl_id
        calls["path"] = path
        calls["kwargs"] = kwargs
        return path

    def fake_from_duckdb(cls, path: str):  # type: ignore[no-untyped-def]
        calls["loaded"] = path
        return sentinel

    monkeypatch.setattr(crawl_module, "export_duckdb_from_db_id", fake_export)
    monkeypatch.setattr(Crawl, "from_duckdb", classmethod(fake_from_duckdb))

    result = Crawl.from_db_id(
        "crawl-123",
        backend="duckdb",
        project_root=str(tmp_path / "projects"),
        duckdb_path=str(target),
        duckdb_tables=("APP.URLS",),
        duckdb_tabs=("internal_all",),
        duckdb_if_exists="skip",
        mapping_path="mapping.json",
        derby_jar="derby.jar",
    )

    assert result is sentinel
    assert calls["crawl_id"] == "crawl-123"
    assert calls["path"] == target
    assert calls["loaded"] == str(target)
    assert calls["kwargs"] == {
        "tables": ("APP.URLS",),
        "tabs": ("internal_all",),
        "if_exists": "skip",
        "project_root": str(tmp_path / "projects"),
        "mapping_path": "mapping.json",
        "derby_jar": "derby.jar",
    }


def test_from_seospider_can_export_and_load_duckdb(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}
    sentinel = object()
    target = tmp_path / "crawl.duckdb"
    project_dir = tmp_path / "project"

    def fake_load_project(*args: object, **kwargs: object) -> Path:
        calls["project_args"] = args
        calls["project_kwargs"] = kwargs
        return project_dir

    def fake_export(project_path: str, path: Path, **kwargs: object) -> Path:
        calls["project_path"] = project_path
        calls["path"] = path
        calls["kwargs"] = kwargs
        return path

    def fake_from_duckdb(cls, path: str):  # type: ignore[no-untyped-def]
        calls["loaded"] = path
        return sentinel

    monkeypatch.setattr(crawl_module, "load_seospider_db_project", fake_load_project)
    monkeypatch.setattr(crawl_module, "export_duckdb_from_derby", fake_export)
    monkeypatch.setattr(Crawl, "from_duckdb", classmethod(fake_from_duckdb))

    result = Crawl.from_seospider(
        str(tmp_path / "crawl.seospider"),
        backend="duckdb",
        duckdb_path=str(target),
        duckdb_tables=("APP.URLS",),
        duckdb_tabs="all",
        duckdb_if_exists="replace",
        mapping_path="mapping.json",
        derby_jar="derby.jar",
    )

    assert result is sentinel
    assert calls["project_path"] == str(project_dir)
    assert calls["path"] == target
    assert calls["loaded"] == str(target)
    assert calls["kwargs"] == {
        "tables": ("APP.URLS",),
        "tabs": "all",
        "if_exists": "replace",
        "source_label": str((tmp_path / "crawl.seospider").resolve()),
        "mapping_path": "mapping.json",
        "derby_jar": "derby.jar",
    }


def test_load_routes_db_id_duckdb_kwargs(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}
    sentinel = object()

    def fake_from_db_id(cls, crawl_id: str, **kwargs: object):  # type: ignore[no-untyped-def]
        calls["crawl_id"] = crawl_id
        calls["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(Crawl, "from_db_id", classmethod(fake_from_db_id))

    result = Crawl.load(
        "crawl-xyz",
        source_type="db_id",
        db_id_backend="duckdb",
        duckdb_path=str(tmp_path / "dbid.duckdb"),
        duckdb_tabs="all",
        duckdb_if_exists="skip",
    )

    assert result is sentinel
    assert calls["crawl_id"] == "crawl-xyz"
    assert calls["kwargs"]["backend"] == "duckdb"
    assert calls["kwargs"]["duckdb_path"] == str(tmp_path / "dbid.duckdb")
    assert calls["kwargs"]["duckdb_tabs"] == "all"
    assert calls["kwargs"]["duckdb_if_exists"] == "skip"


def test_load_routes_seospider_duckdb_kwargs(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}
    sentinel = object()
    crawl_path = tmp_path / "crawl.seospider"
    crawl_path.write_text("stub", encoding="utf-8")

    def fake_from_seospider(cls, path: str, **kwargs: object):  # type: ignore[no-untyped-def]
        calls["path"] = path
        calls["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(Crawl, "from_seospider", classmethod(fake_from_seospider))

    result = Crawl.load(
        str(crawl_path),
        source_type="seospider",
        seospider_backend="duckdb",
        duckdb_path=str(tmp_path / "seospider.duckdb"),
        duckdb_tables=("APP.URLS", "APP.LINKS"),
        duckdb_if_exists="skip",
    )

    assert result is sentinel
    assert calls["path"] == str(crawl_path)
    assert calls["kwargs"]["backend"] == "duckdb"
    assert calls["kwargs"]["duckdb_path"] == str(tmp_path / "seospider.duckdb")
    assert calls["kwargs"]["duckdb_tables"] == ("APP.URLS", "APP.LINKS")
    assert calls["kwargs"]["duckdb_if_exists"] == "skip"


def test_load_dbseospider_defaults_to_duckdb_backend(tmp_path: Path, monkeypatch) -> None:
    calls: dict[str, object] = {}
    sentinel = object()
    crawl_path = tmp_path / "crawl.dbseospider"
    crawl_path.write_text("stub", encoding="utf-8")

    monkeypatch.setattr(crawl_module, "_looks_like_sqlite", lambda path: False)

    def fake_from_derby(cls, path: str, **kwargs: object):  # type: ignore[no-untyped-def]
        calls["path"] = path
        calls["kwargs"] = kwargs
        return sentinel

    monkeypatch.setattr(Crawl, "from_derby", classmethod(fake_from_derby))

    result = Crawl.load(str(crawl_path))

    assert result is sentinel
    assert calls["path"] == str(crawl_path)
    assert calls["kwargs"]["backend"] == "duckdb"
    assert calls["kwargs"]["duckdb_if_exists"] == "auto"
