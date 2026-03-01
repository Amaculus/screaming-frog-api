from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class InternalRow:
    id: Optional[int]
    address: str
    status_code: Optional[int]
