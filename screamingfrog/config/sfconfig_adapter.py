from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .patches import ConfigPatches


def write_seospider_config(
    template_config: str | Path,
    output_config: str | Path,
    patches: ConfigPatches | Mapping[str, Any],
    *,
    sf_path: str | None = None,
) -> Path:
    """Apply patches to a template .seospiderconfig file and save output."""
    sf_config_cls = _load_sfconfig_class()
    config = sf_config_cls.load(str(template_config), sf_path=sf_path)

    payload = patches.to_dict() if isinstance(patches, ConfigPatches) else dict(patches)

    for path, value in payload.items():
        if path in {"extractions", "custom_searches", "custom_javascript"}:
            continue
        config.set(path, value)

    for op in payload.get("extractions", []):
        _apply_extraction_op(config, op)
    for op in payload.get("custom_searches", []):
        _apply_custom_search_op(config, op)
    for op in payload.get("custom_javascript", []):
        _apply_custom_javascript_op(config, op)

    config.save(str(output_config))
    return Path(output_config)


def _load_sfconfig_class():
    try:
        from sfconfig import SFConfig
    except Exception as exc:  # pragma: no cover - exercised in tests via monkeypatch
        raise RuntimeError(
            "sf-config-builder is required for writing .seospiderconfig files. "
            "Install it with: pip install sf-config-builder"
        ) from exc
    return SFConfig


def _apply_extraction_op(config: Any, op: Mapping[str, Any]) -> None:
    action = str(op.get("op", "")).lower()
    if action == "add":
        config.add_extraction(
            name=str(op["name"]),
            selector=str(op["selector"]),
            selector_type=str(op.get("selectorType", "XPATH")),
            extract_mode=str(op.get("extractMode", "TEXT")),
            attribute=op.get("attribute"),
        )
        return
    if action == "remove":
        config.remove_extraction(str(op["name"]))
        return
    if action == "clear":
        config.clear_extractions()
        return
    raise ValueError(f"Unsupported extraction op: {op}")


def _apply_custom_search_op(config: Any, op: Mapping[str, Any]) -> None:
    action = str(op.get("op", "")).lower()
    if action == "add":
        config.add_custom_search(
            name=str(op["name"]),
            query=str(op["query"]),
            mode=str(op.get("mode", "CONTAINS")),
            data_type=str(op.get("dataType", "TEXT")),
            scope=str(op.get("scope", "HTML")),
            case_sensitive=bool(op.get("caseSensitive", False)),
            xpath=op.get("xpath"),
        )
        return
    if action == "remove":
        config.remove_custom_search(str(op["name"]))
        return
    if action == "clear":
        config.clear_custom_searches()
        return
    raise ValueError(f"Unsupported custom_search op: {op}")


def _apply_custom_javascript_op(config: Any, op: Mapping[str, Any]) -> None:
    action = str(op.get("op", "")).lower()
    if action == "add":
        config.add_custom_javascript(
            name=str(op["name"]),
            javascript=str(op["javascript"]),
            script_type=str(op.get("type", "EXTRACTION")),
            timeout_secs=int(op.get("timeout_secs", 10)),
            content_types=str(op.get("content_types", "text/html")),
        )
        return
    if action == "remove":
        config.remove_custom_javascript(str(op["name"]))
        return
    if action == "clear":
        config.clear_custom_javascript()
        return
    raise ValueError(f"Unsupported custom_javascript op: {op}")
