from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_page_title_filters() -> None:
    filters = [
        FilterDef(
            name="All",
            tab="Page Titles",
            description="All page titles.",
        ),
        FilterDef(
            name="Missing",
            tab="Page Titles",
            description="Missing title tag.",
            sql_where="TITLE_1 IS NULL OR TRIM(TITLE_1) = ''",
            columns=["Title 1"],
        ),
        FilterDef(
            name="Duplicate",
            tab="Page Titles",
            description="Duplicate title tag text.",
            sql_where="j.DUPLICATE_KEY IS NOT NULL",
            join_table="APP.DUPLICATES_TITLE",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Over X Characters",
            tab="Page Titles",
            description="Title length over 60 characters.",
            sql_where="TITLE_1 IS NOT NULL AND LENGTH(TITLE_1) > 60",
            columns=["Title 1"],
        ),
        FilterDef(
            name="Below X Characters",
            tab="Page Titles",
            description="Title length below 30 characters.",
            sql_where="TITLE_1 IS NOT NULL AND LENGTH(TITLE_1) < 30",
            columns=["Title 1"],
        ),
        FilterDef(
            name="Over X Pixels",
            tab="Page Titles",
            description="Title pixel width over threshold (TODO: DB column).",
        ),
        FilterDef(
            name="Below X Pixels",
            tab="Page Titles",
            description="Title pixel width below threshold (TODO: DB column).",
        ),
        FilterDef(
            name="Same as H1",
            tab="Page Titles",
            description="Title text matches H1.",
            sql_where=(
                "TITLE_1 IS NOT NULL AND H1_1 IS NOT NULL AND TITLE_1 = H1_1"
            ),
            columns=["Title 1", "H1-1"],
        ),
        FilterDef(
            name="Multiple",
            tab="Page Titles",
            description="Multiple title tags on the page.",
            sql_where="NUM_TITLES > 1",
            columns=["Title 1", "Title 2"],
        ),
        FilterDef(
            name="Outside <head>",
            tab="Page Titles",
            description="Title tag outside <head>.",
            sql_where="j.TITLE_OUTSIDE_HEAD = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
    ]
    for filt in filters:
        register_filter(filt)


register_page_title_filters()
