"""The mapped "Indexability" expression must follow the response code.

Screaming Frog reports every non-2xx URL as Non-Indexable ("Redirected",
"Client Error", "Server Error", "No Response"). The mapped expression used to
test only robots.txt and noindex, so a 301 or a 404 read as Indexable, and any
consumer filtering on Indexable pages counted redirects and errors as pages.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

_MAPPING = json.loads(
    (Path(__file__).resolve().parents[1] / "screamingfrog" / "resources" / "mapping.json").read_text(
        encoding="utf-8"
    )
)


def _expressions(column: str) -> list[tuple[str, str]]:
    return [
        (tab, entry["db_expression"])
        for tab, entries in _MAPPING.items()
        for entry in entries
        if entry.get("csv_column") == column and entry.get("db_expression")
    ]


def _urls_table() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    columns = ["ENCODED_URL TEXT", "RESPONSE_CODE INTEGER", "BLOCKED_BY_ROBOTS_TXT INTEGER"]
    for index in range(1, 21):
        columns += [
            f"META_NAME_{index} TEXT",
            f"META_CONTENT_{index} TEXT",
            f"META_NAME_JS_{index} TEXT",
            f"META_CONTENT_JS_{index} TEXT",
            f"X_ROBOT_TAG_{index} TEXT",
        ]
    conn.execute(f"CREATE TABLE URLS ({', '.join(columns)})")
    conn.executemany(
        "INSERT INTO URLS (ENCODED_URL, RESPONSE_CODE, BLOCKED_BY_ROBOTS_TXT, META_NAME_1, META_CONTENT_1) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("ok", 200, 0, None, None),
            ("noindex", 200, 0, "robots", "noindex, follow"),
            ("moved", 301, 0, None, None),
            ("gone", 404, 0, None, None),
            ("broken", 503, 0, None, None),
            ("timeout", 0, 0, None, None),
            ("blocked", 0, 1, None, None),
            ("unknown", None, 0, None, None),
        ],
    )
    return conn


def _evaluate(expression: str) -> dict[str, str | None]:
    conn = _urls_table()
    rows = conn.execute(f"SELECT ENCODED_URL, {expression} FROM URLS").fetchall()
    conn.close()
    return dict(rows)


def test_every_indexability_expression_carries_the_status_branch() -> None:
    for column in ("Indexability", "Indexability Status"):
        expressions = _expressions(column)
        assert expressions, column
        missing = [tab for tab, expression in expressions if "RESPONSE_CODE" not in expression]
        assert missing == [], f"{column} without a response-code branch: {missing[:5]}"


@pytest.mark.parametrize("tab", ["internal_all.csv", "response_codes_all.csv"])
def test_a_non_2xx_url_is_non_indexable(tab: str) -> None:
    by_column = {
        entry["csv_column"]: entry["db_expression"]
        for entry in _MAPPING[tab]
        if entry.get("csv_column") in {"Indexability", "Indexability Status"}
    }

    indexability = _evaluate(by_column["Indexability"])
    status = _evaluate(by_column["Indexability Status"])

    assert indexability == {
        "ok": "Indexable",
        "noindex": "Non-Indexable",
        "moved": "Non-Indexable",
        "gone": "Non-Indexable",
        "broken": "Non-Indexable",
        "timeout": "Non-Indexable",
        "blocked": "Non-Indexable",
        "unknown": "Indexable",
    }
    assert status == {
        "ok": None,
        "noindex": "noindex",
        "moved": "Redirected",
        "gone": "Client Error",
        "broken": "Server Error",
        "timeout": "No Response",
        "blocked": "blocked by robots.txt",
        "unknown": None,
    }
