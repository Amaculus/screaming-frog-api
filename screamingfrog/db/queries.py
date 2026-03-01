from __future__ import annotations

from typing import Any


def build_where(filters: dict[str, Any], column_map: dict[str, str]) -> tuple[str, list[Any]]:
    """Build a SQL WHERE clause and parameter list from simple equality filters."""
    clauses = []
    params: list[Any] = []
    for key, expected in filters.items():
        column = column_map.get(key, key)
        if isinstance(expected, (list, tuple, set)):
            placeholders = ", ".join(["?"] * len(expected))
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(list(expected))
        elif expected is None:
            clauses.append(f"{column} IS NULL")
        else:
            clauses.append(f"{column} = ?")
            params.append(expected)
    return " AND ".join(clauses), params
