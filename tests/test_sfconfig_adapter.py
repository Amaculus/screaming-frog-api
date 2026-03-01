from __future__ import annotations

from pathlib import Path

import pytest

from screamingfrog.config import (
    ConfigPatches,
    CustomJavaScript,
    CustomSearch,
    write_seospider_config,
)
from screamingfrog.config import sfconfig_adapter


class _FakeConfig:
    loaded: tuple[str, str | None, "_FakeConfig"] | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []
        self.saved: str | None = None

    @classmethod
    def load(cls, path: str, sf_path: str | None = None) -> "_FakeConfig":
        inst = cls()
        cls.loaded = (path, sf_path, inst)
        return inst

    def set(self, path: str, value):
        self.calls.append(("set", (path, value), {}))
        return self

    def add_extraction(self, **kwargs):
        self.calls.append(("add_extraction", tuple(), kwargs))
        return self

    def remove_extraction(self, name: str):
        self.calls.append(("remove_extraction", (name,), {}))
        return self

    def clear_extractions(self):
        self.calls.append(("clear_extractions", tuple(), {}))
        return self

    def add_custom_search(self, **kwargs):
        self.calls.append(("add_custom_search", tuple(), kwargs))
        return self

    def remove_custom_search(self, name: str):
        self.calls.append(("remove_custom_search", (name,), {}))
        return self

    def clear_custom_searches(self):
        self.calls.append(("clear_custom_searches", tuple(), {}))
        return self

    def add_custom_javascript(self, **kwargs):
        self.calls.append(("add_custom_javascript", tuple(), kwargs))
        return self

    def remove_custom_javascript(self, name: str):
        self.calls.append(("remove_custom_javascript", (name,), {}))
        return self

    def clear_custom_javascript(self):
        self.calls.append(("clear_custom_javascript", tuple(), {}))
        return self

    def save(self, path: str):
        self.saved = path
        return self


def test_write_seospider_config_with_configpatches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sfconfig_adapter, "_load_sfconfig_class", lambda: _FakeConfig)

    patches = (
        ConfigPatches()
        .set("mCrawlConfig.mMaxUrls", 5000)
        .add_extraction("Price", "//span[@class='price']")
        .add_custom_search(
            CustomSearch(
                name="Has Login",
                query="login",
                mode="contains",
                data_type="text",
                scope="html",
            )
        )
        .add_custom_javascript(
            CustomJavaScript(
                name="Title",
                javascript="return document.title;",
            )
        )
    )

    out = write_seospider_config(
        "base.seospiderconfig",
        "alpha.seospiderconfig",
        patches,
        sf_path="C:/Program Files (x86)/Screaming Frog SEO Spider",
    )

    assert out == Path("alpha.seospiderconfig")
    assert _FakeConfig.loaded is not None
    loaded_path, loaded_sf_path, cfg = _FakeConfig.loaded
    assert loaded_path == "base.seospiderconfig"
    assert loaded_sf_path == "C:/Program Files (x86)/Screaming Frog SEO Spider"
    assert cfg.saved == "alpha.seospiderconfig"
    assert ("set", ("mCrawlConfig.mMaxUrls", 5000), {}) in cfg.calls
    assert any(name == "add_extraction" for name, *_ in cfg.calls)
    assert any(name == "add_custom_search" for name, *_ in cfg.calls)
    assert any(name == "add_custom_javascript" for name, *_ in cfg.calls)


def test_write_seospider_config_supports_remove_clear_ops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sfconfig_adapter, "_load_sfconfig_class", lambda: _FakeConfig)

    payload = {
        "mCrawlConfig.mMaxDepth": 3,
        "extractions": [{"op": "remove", "name": "Old Rule"}, {"op": "clear"}],
        "custom_searches": [{"op": "remove", "name": "Old Search"}, {"op": "clear"}],
        "custom_javascript": [{"op": "remove", "name": "Old JS"}, {"op": "clear"}],
    }

    write_seospider_config("base.seospiderconfig", "alpha.seospiderconfig", payload)

    assert _FakeConfig.loaded is not None
    _, _, cfg = _FakeConfig.loaded
    assert ("set", ("mCrawlConfig.mMaxDepth", 3), {}) in cfg.calls
    assert ("remove_extraction", ("Old Rule",), {}) in cfg.calls
    assert ("clear_extractions", tuple(), {}) in cfg.calls
    assert ("remove_custom_search", ("Old Search",), {}) in cfg.calls
    assert ("clear_custom_searches", tuple(), {}) in cfg.calls
    assert ("remove_custom_javascript", ("Old JS",), {}) in cfg.calls
    assert ("clear_custom_javascript", tuple(), {}) in cfg.calls


def test_write_seospider_config_raises_on_invalid_op(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sfconfig_adapter, "_load_sfconfig_class", lambda: _FakeConfig)

    with pytest.raises(ValueError):
        write_seospider_config(
            "base.seospiderconfig",
            "alpha.seospiderconfig",
            {"custom_searches": [{"op": "noop"}]},
        )
