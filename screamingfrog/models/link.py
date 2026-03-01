from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class Link:
    """Generic link row for inlinks/outlinks."""

    source: Optional[str]
    destination: Optional[str]
    anchor_text: Optional[str] = None
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> "Link":
        return cls(
            source=_get_str(row, "Source"),
            destination=_get_str(row, "Destination"),
            anchor_text=_get_str(row, "Anchor"),
            data=dict(row),
        )


def _get_str(row: Mapping[str, Any], key: str) -> Optional[str]:
    value = row.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None
