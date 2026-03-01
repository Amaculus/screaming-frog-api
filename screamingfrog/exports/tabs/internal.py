from __future__ import annotations

from typing import Any, Mapping

from screamingfrog.models import InternalPage


def parse_internal_row(row: Mapping[str, Any]) -> InternalPage:
    """Parse a CSV internal row into an InternalPage."""
    return InternalPage.from_csv_row(row)
