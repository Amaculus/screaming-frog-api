from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class CustomSearch:
    """Custom search rule for ConfigBuilder patches."""

    name: str
    query: str
    mode: str = "CONTAINS"
    data_type: str = "TEXT"
    scope: str = "HTML"
    case_sensitive: bool = False
    xpath: Optional[str] = None

    def to_op(self) -> Dict[str, Any]:
        op: Dict[str, Any] = {
            "op": "add",
            "name": self.name,
            "query": self.query,
            "mode": self.mode,
            "dataType": self.data_type,
            "scope": self.scope,
            "caseSensitive": self.case_sensitive,
        }
        if self.xpath:
            op["xpath"] = self.xpath
        return op


@dataclass(frozen=True)
class CustomJavaScript:
    """Custom JavaScript rule for ConfigBuilder patches."""

    name: str
    javascript: str
    type: str = "EXTRACTION"
    timeout_secs: int = 10
    content_types: str = "text/html"

    def to_op(self) -> Dict[str, Any]:
        return {
            "op": "add",
            "name": self.name,
            "javascript": self.javascript,
            "type": self.type,
            "timeout_secs": self.timeout_secs,
            "content_types": self.content_types,
        }


@dataclass
class ConfigPatches:
    """Helper to build ConfigBuilder patches JSON."""

    _patches: Dict[str, Any] = field(default_factory=dict)
    _extractions: List[Dict[str, Any]] = field(default_factory=list)
    _custom_searches: List[Dict[str, Any]] = field(default_factory=list)
    _custom_javascript: List[Dict[str, Any]] = field(default_factory=list)

    def set(self, path: str, value: Any) -> "ConfigPatches":
        self._patches[path] = value
        return self

    # ---- Extractions ----
    def add_extraction(
        self,
        name: str,
        selector: str,
        selector_type: str = "XPATH",
        extract_mode: str = "TEXT",
        attribute: Optional[str] = None,
    ) -> "ConfigPatches":
        op: Dict[str, Any] = {
            "op": "add",
            "name": name,
            "selector": selector,
            "selectorType": selector_type,
            "extractMode": extract_mode,
        }
        if attribute:
            op["attribute"] = attribute
        self._extractions.append(op)
        return self

    def remove_extraction(self, name: str) -> "ConfigPatches":
        self._extractions.append({"op": "remove", "name": name})
        return self

    def clear_extractions(self) -> "ConfigPatches":
        self._extractions.append({"op": "clear"})
        return self

    # ---- Custom Searches ----
    def add_custom_search(self, rule: CustomSearch) -> "ConfigPatches":
        self._custom_searches.append(rule.to_op())
        return self

    def remove_custom_search(self, name: str) -> "ConfigPatches":
        self._custom_searches.append({"op": "remove", "name": name})
        return self

    def clear_custom_searches(self) -> "ConfigPatches":
        self._custom_searches.append({"op": "clear"})
        return self

    # ---- Custom JavaScript ----
    def add_custom_javascript(self, rule: CustomJavaScript) -> "ConfigPatches":
        self._custom_javascript.append(rule.to_op())
        return self

    def remove_custom_javascript(self, name: str) -> "ConfigPatches":
        self._custom_javascript.append({"op": "remove", "name": name})
        return self

    def clear_custom_javascript(self) -> "ConfigPatches":
        self._custom_javascript.append({"op": "clear"})
        return self

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = dict(self._patches)
        if self._extractions:
            payload["extractions"] = list(self._extractions)
        if self._custom_searches:
            payload["custom_searches"] = list(self._custom_searches)
        if self._custom_javascript:
            payload["custom_javascript"] = list(self._custom_javascript)
        return payload

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)
