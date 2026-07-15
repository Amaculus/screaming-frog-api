"""Canonical Python-side filter-value matching shared by all backends.

Post-filter semantics were previously inconsistent across backends:
Derby trimmed, lowercased and int-coerced (truncating floats), while
DuckDB and CSV compared raw strings case-sensitively, so the same
filter dict returned different rows depending on backend and code path.
Every backend's Python-side filter path routes through
:func:`filter_value_matches` so the semantics live in one place.
"""

from __future__ import annotations

from typing import Any

_TRUE_TOKENS = {"true", "1", "yes", "y", "on"}
_FALSE_TOKENS = {"false", "0", "no", "n", "off"}


def is_blank(value: Any) -> bool:
    """True for NULL-ish values: None or a blank/whitespace-only string."""
    return value is None or (isinstance(value, str) and not value.strip())


def _as_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _TRUE_TOKENS:
            return True
        if token in _FALSE_TOKENS:
            return False
    return None


def filter_value_matches(actual: Any, expected: Any) -> bool:
    """Return True when a row value satisfies a filter value.

    Semantics (in precedence order):

    - a callable is a predicate over the raw row value
    - a list/tuple/set matches when any member matches
    - ``None`` matches NULL or blank strings
    - a bool matches recognised boolean tokens only (``"maybe"`` is not True)
    - numeric values compare numerically without int truncation
      (``0.1`` does not match ``0.9``; ``"404"`` matches ``404``)
    - everything else compares trimmed and case-insensitively
    """
    if callable(expected):
        try:
            return bool(expected(actual))
        except Exception:
            return False
    if isinstance(expected, (list, tuple, set, frozenset)):
        return any(filter_value_matches(actual, item) for item in expected)
    if expected is None:
        return is_blank(actual)
    if isinstance(expected, bool):
        actual_bool = _as_bool(actual)
        return actual_bool is not None and actual_bool == expected
    expected_num = _as_number(expected)
    if expected_num is not None:
        actual_num = _as_number(actual)
        return actual_num is not None and actual_num == expected_num
    if actual is None:
        return False
    return str(actual).strip().lower() == str(expected).strip().lower()
