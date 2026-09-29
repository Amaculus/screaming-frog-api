"""The absent-column guard must not read SQL words as column names.

`_expression_references_absent_column` drops a mapped expression, and the tab
then returns NULL for that column, when the expression names a column the
crawl's schema lacks. It finds bare identifiers by regex and skips the words in
`_SQL_IDENTIFIERS_TO_IGNORE`. A SQL word missing from that set is read as a
column the table does not have, so every expression using it is dropped on
every crawl, silently. Before this test, LONG (CAST ... AS LONG VARCHAR)
emptied all custom extraction columns, BOTH (TRIM(BOTH ...)) emptied the
inlink/outlink "Rel", and OFFSET / ROW / NEXT / BY emptied the second hreflang
slot.

`fixtures/sf_24_3_derby_columns.json` is the APP schema of a real Screaming
Frog 24.3 crawl: table and column names only.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from screamingfrog.backends.derby_backend import (
    _DOTTED_COLUMN_RE,
    _IDENTIFIER_RE,
    _SQL_IDENTIFIERS_TO_IGNORE,
    _STRING_LITERAL_RE,
    _expression_references_absent_column,
)

_ROOT = Path(__file__).resolve().parents[1]
_MAPPING: dict[str, list[dict]] = json.loads(
    (_ROOT / "screamingfrog" / "resources" / "mapping.json").read_text(encoding="utf-8")
)
_SCHEMA: dict[str, frozenset[str]] = {
    table: frozenset(columns)
    for table, columns in json.loads(
        (_ROOT / "tests" / "fixtures" / "sf_24_3_derby_columns.json").read_text(encoding="utf-8")
    )["tables"].items()
}

# SQL reserved words (SQL:2016 and Derby) that can appear in a scalar
# expression. None is a column in the SF schema.
_SQL_WORDS = frozenset(
    """
    ALL AND ANY AS ASC BETWEEN BIGINT BOOLEAN BOTH BY CASE CAST CHAR CHARACTER
    COALESCE CROSS CURRENT DATE DECIMAL DESC DISTINCT DOUBLE ELSE END ESCAPE
    EXCEPT EXISTS FALSE FETCH FIRST FLOAT FOR FROM FULL GROUP HAVING IN INNER
    INT INTEGER INTERSECT IS JOIN LEADING LEFT LIKE LIMIT LONG NATURAL NEXT NOT
    NULL NULLIF OFFSET ON ONLY OR ORDER OUTER OVER PARTITION REAL RIGHT ROW ROWS
    SELECT SMALLINT SOME THEN TIMESTAMP TRAILING TRUE UNION UNKNOWN USING
    VALUES VARCHAR WHEN WHERE WITH
    """.split()
)


def _bare_words(expression: str) -> set[str]:
    text = _DOTTED_COLUMN_RE.sub(" ", _STRING_LITERAL_RE.sub(" ", expression))
    words: set[str] = set()
    for match in _IDENTIFIER_RE.finditer(text):
        if text[match.end() : match.end() + 1] == "(":
            continue  # a function call, which the guard already skips
        words.add(match.group(1).upper())
    return words


def test_every_sql_word_in_a_mapped_expression_is_ignored() -> None:
    unignored: dict[str, str] = {}
    for tab, entries in _MAPPING.items():
        for entry in entries:
            expression = entry.get("db_expression")
            if not expression:
                continue
            for word in _bare_words(expression) & _SQL_WORDS:
                if word not in _SQL_IDENTIFIERS_TO_IGNORE:
                    unignored.setdefault(word, f"{tab} / {entry['csv_column']}")
    assert unignored == {}, (
        "SQL words the absent-column guard reads as columns (word: first use): "
        f"{unignored}"
    )


# Ignored words that are ALSO a real column, each with the reason it is safe.
_KNOWN_OVERLAPS = {
    "TIMESTAMP": (
        "APP.URLS.TIMESTAMP exists on every SF version this library reads, so "
        "never checking it for absence cannot hide a missing column"
    ),
}


def test_no_ignored_word_is_a_real_column_without_a_reason() -> None:
    # Ignoring a word that is also a column hides that column's absence on a
    # schema that lacks it, so every such overlap must be named and justified.
    columns = set().union(*_SCHEMA.values())
    overlaps = _SQL_IDENTIFIERS_TO_IGNORE & columns
    assert overlaps == set(_KNOWN_OVERLAPS)
    assert all(reason.strip() for reason in _KNOWN_OVERLAPS.values())


@pytest.mark.parametrize(
    ("tab", "column"),
    [
        ("hreflang_all.csv", "HTML hreflang 2"),
        ("hreflang_all.csv", "HTML hreflang 2 URL"),
        ("internal_all.csv", "Extractor 1 1"),
        ("all_inlinks.csv", "Rel"),
        ("all_outlinks.csv", "Rel"),
    ],
)
def test_columns_the_guard_used_to_drop_survive_a_real_schema(tab: str, column: str) -> None:
    entry = next(e for e in _MAPPING[tab] if e["csv_column"] == column)

    dropped = _expression_references_absent_column(
        entry["db_expression"],
        _SCHEMA,
        default_table=entry.get("db_table") or "APP.URLS",
    )

    assert dropped is False
