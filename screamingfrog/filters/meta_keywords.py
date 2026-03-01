from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


_META_NAME_COLUMNS = [f"META_NAME_{i}" for i in range(1, 21)]
_META_CONTENT_COLUMNS = [f"META_CONTENT_{i}" for i in range(1, 21)]


def _name_is_keywords(col: str) -> str:
    return f"LOWER({col}) = 'keywords'"


def _keywords_exists_expr() -> str:
    return "(" + " OR ".join(_name_is_keywords(col) for col in _META_NAME_COLUMNS) + ")"


def _keywords_count_expr() -> str:
    parts = [f"CASE WHEN {_name_is_keywords(col)} THEN 1 ELSE 0 END" for col in _META_NAME_COLUMNS]
    return "(" + " + ".join(parts) + ")"


def _keywords_value_expr() -> str:
    parts = [
        f"CASE WHEN {_name_is_keywords(name)} THEN NULLIF({content}, '') END"
        for name, content in zip(_META_NAME_COLUMNS, _META_CONTENT_COLUMNS)
    ]
    return "COALESCE(" + ", ".join(parts) + ")"


def register_meta_keywords_filters() -> None:
    keywords_value = _keywords_value_expr()
    keywords_exists = _keywords_exists_expr()
    keywords_count = _keywords_count_expr()

    filters = [
        FilterDef(name="All", tab="Meta Keywords", description="All meta keywords."),
        FilterDef(
            name="Missing",
            tab="Meta Keywords",
            description="Missing meta keywords tag.",
            sql_where=f"NOT {keywords_exists}",
        ),
        FilterDef(
            name="Duplicate",
            tab="Meta Keywords",
            description="Duplicate meta keywords text.",
            sql_where=(
                f"{keywords_value} IN (SELECT {keywords_value} FROM APP.URLS "
                f"WHERE {keywords_value} IS NOT NULL GROUP BY {keywords_value} HAVING COUNT(*) > 1)"
            ),
        ),
        FilterDef(
            name="Multiple",
            tab="Meta Keywords",
            description="Multiple meta keywords tags.",
            sql_where=f"{keywords_count} > 1",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_meta_keywords_filters()
