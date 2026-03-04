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
        return cls.from_data(row)

    @classmethod
    def from_db_row(cls, columns: list[str], values: tuple[Any, ...]) -> "InternalPage":
        return cls.from_data({col: val for col, val in zip(columns, values)})

    @classmethod
    def from_data(
        cls,
        data: Mapping[str, Any],
        *,
        copy_data: bool = True,
    ) -> "InternalPage":
        page_data = dict(data) if copy_data or not isinstance(data, dict) else data
        address = str(
            page_data.get("address")
            or page_data.get("Address")
            or page_data.get("ADDRESS")
            or page_data.get("encoded_url")
            or page_data.get("ENCODED_URL")
            or ""
        ).strip()
        status_code = _to_int(
            page_data.get("status_code")
            or page_data.get("Status Code")
            or page_data.get("response_code")
            or page_data.get("RESPONSE_CODE")
        )
        return cls(
            address=address,
            status_code=status_code,
            id=_to_int(page_data.get("id") or page_data.get("ID")),
            data=page_data,
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
