from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional


@dataclass(frozen=True)
class InternalPage:
    """Represents a single row from the Internal tab/table."""

    address: str
    status_code: Optional[int] = None
    id: Optional[int] = None
    data: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_csv_row(cls, row: Mapping[str, Any]) -> "InternalPage":
        address = str(row.get("Address") or "").strip()
        status_code = _to_int(row.get("Status Code"))
        return cls(address=address, status_code=status_code, data=dict(row))

    @classmethod
    def from_db_row(cls, columns: list[str], values: tuple[Any, ...]) -> "InternalPage":
        data = {col: val for col, val in zip(columns, values)}
        address = str(
            data.get("address")
            or data.get("Address")
            or data.get("ADDRESS")
            or data.get("encoded_url")
            or data.get("ENCODED_URL")
            or ""
        ).strip()
        status_code = _to_int(
            data.get("status_code")
            or data.get("Status Code")
            or data.get("response_code")
            or data.get("RESPONSE_CODE")
        )
        return cls(
            address=address,
            status_code=status_code,
            id=_to_int(data.get("id") or data.get("ID")),
            data=data,
        )


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        if isinstance(value, str) and value.strip() == "":
            return None
        return int(value)
    except (ValueError, TypeError):
        return None
