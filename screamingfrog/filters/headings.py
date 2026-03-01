from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_heading_filters() -> None:
    filters = [
        # H1
        FilterDef(name="All", tab="H1", description="All H1 entries."),
        FilterDef(
            name="Missing",
            tab="H1",
            description="Missing H1.",
            sql_where="H1_1 IS NULL OR TRIM(H1_1) = ''",
        ),
        FilterDef(
            name="Duplicate",
            tab="H1",
            description="Duplicate H1 text.",
            sql_where="j.DUPLICATE_KEY IS NOT NULL",
            join_table="APP.DUPLICATES_H1",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Over X Characters",
            tab="H1",
            description="H1 length over threshold (default 70).",
            sql_where="H1_1 IS NOT NULL AND LENGTH(H1_1) > 70",
        ),
        FilterDef(
            name="Multiple",
            tab="H1",
            description="Multiple H1 tags.",
            sql_where="NUM_H1 > 1",
        ),
        FilterDef(
            name="Alt Text in H1",
            tab="H1",
            description="H1 sourced from image alt text.",
            sql_where=(
                "H1_SOURCE_1 = 'IMG_ALT' OR H1_SOURCE_2 = 'IMG_ALT' "
                "OR H1_JS_SOURCE_1 = 'IMG_ALT' OR H1_JS_SOURCE_2 = 'IMG_ALT'"
            ),
        ),
        FilterDef(
            name="Non-Sequential",
            tab="H1",
            description="Non-sequential heading order for H1.",
            sql_where="NON_SEQUENTIAL_H1 = 1",
        ),
        # H2
        FilterDef(name="All", tab="H2", description="All H2 entries."),
        FilterDef(
            name="Missing",
            tab="H2",
            description="Missing H2.",
            sql_where="H2_1 IS NULL OR TRIM(H2_1) = ''",
        ),
        FilterDef(
            name="Duplicate",
            tab="H2",
            description="Duplicate H2 text.",
            sql_where="j.DUPLICATE_KEY IS NOT NULL",
            join_table="APP.DUPLICATES_H2",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Over X Characters",
            tab="H2",
            description="H2 length over threshold (default 70).",
            sql_where="H2_1 IS NOT NULL AND LENGTH(H2_1) > 70",
        ),
        FilterDef(
            name="Multiple",
            tab="H2",
            description="Multiple H2 tags.",
            sql_where="NUM_H2 > 1",
        ),
        FilterDef(
            name="Non-Sequential",
            tab="H2",
            description="Non-sequential heading order for H2.",
            sql_where="NON_SEQUENTIAL_H2 = 1",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_heading_filters()
