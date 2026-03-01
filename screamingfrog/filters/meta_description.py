from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


_META_NAME_COLUMNS = [f"META_NAME_{i}" for i in range(1, 21)]
_META_CONTENT_COLUMNS = [f"META_CONTENT_{i}" for i in range(1, 21)]
_META_NAME_JS_COLUMNS = [f"META_NAME_JS_{i}" for i in range(1, 21)]
_META_CONTENT_JS_COLUMNS = [f"META_CONTENT_JS_{i}" for i in range(1, 21)]


def _name_is_description(col: str) -> str:
    return f"LOWER({col}) = 'description'"


def _description_exists_expr() -> str:
    return "(" + " OR ".join(
        _name_is_description(col) for col in (_META_NAME_COLUMNS + _META_NAME_JS_COLUMNS)
    ) + ")"


def _description_count_expr() -> str:
    parts = [
        f"CASE WHEN {_name_is_description(col)} THEN 1 ELSE 0 END"
        for col in (_META_NAME_COLUMNS + _META_NAME_JS_COLUMNS)
    ]
    return "(" + " + ".join(parts) + ")"


def _description_value_expr() -> str:
    names = _META_NAME_COLUMNS + _META_NAME_JS_COLUMNS
    contents = _META_CONTENT_COLUMNS + _META_CONTENT_JS_COLUMNS
    parts = [
        f"CASE WHEN {_name_is_description(name)} THEN NULLIF({content}, '') END"
        for name, content in zip(names, contents)
    ]
    return "COALESCE(" + ", ".join(parts) + ")"


def register_meta_description_filters() -> None:
    desc_value = _description_value_expr()
    desc_exists = _description_exists_expr()
    desc_count = _description_count_expr()

    filters = [
        FilterDef(
            name="All",
            tab="Meta Description",
            description="All meta descriptions.",
        ),
        FilterDef(
            name="Missing",
            tab="Meta Description",
            description="Missing meta description tag.",
            sql_where=f"NOT {desc_exists}",
        ),
        FilterDef(
            name="Duplicate",
            tab="Meta Description",
            description="Duplicate meta description text.",
            sql_where="j.DUPLICATE_KEY IS NOT NULL",
            join_table="APP.DUPLICATES_META_DESCRIPTION",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Over X Characters",
            tab="Meta Description",
            description="Meta description over 155 characters.",
            sql_where=f"{desc_value} IS NOT NULL AND LENGTH({desc_value}) > 155",
        ),
        FilterDef(
            name="Below X Characters",
            tab="Meta Description",
            description="Meta description below 70 characters.",
            sql_where=f"{desc_value} IS NOT NULL AND LENGTH({desc_value}) < 70",
        ),
        FilterDef(
            name="Over X Pixels",
            tab="Meta Description",
            description="Meta description over pixel threshold (TODO: DB column).",
        ),
        FilterDef(
            name="Below X Pixels",
            tab="Meta Description",
            description="Meta description below pixel threshold (TODO: DB column).",
        ),
        FilterDef(
            name="Multiple",
            tab="Meta Description",
            description="Multiple meta description tags.",
            sql_where=f"{desc_count} > 1",
        ),
        FilterDef(
            name="Outside <head>",
            tab="Meta Description",
            description="Meta description outside <head>.",
            sql_where="j.META_DESCRIPTION_OUTSIDE_HEAD = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_meta_description_filters()
