from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_image_filters() -> None:
    filters = [
        FilterDef(
            name="All",
            tab="Images",
            description="All image URLs.",
            sql_where="CONTENT_TYPE LIKE 'image/%'",
        ),
        FilterDef(
            name="Over X KB",
            tab="Images",
            description="Images over 100KB (approx via PAGE_SIZE).",
            sql_where="CONTENT_TYPE LIKE 'image/%' AND PAGE_SIZE > 102400",
        ),
        FilterDef(
            name="Missing Alt Text",
            tab="Images",
            description="Missing alt text (tracker).",
            sql_where="j.BAD_SRC_COUNT > 0",
            join_table="APP.MISSING_ALT_TEXT_TRACKER",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Missing Alt Attribute",
            tab="Images",
            description="Missing alt attribute (tracker).",
            sql_where="j.BAD_SRC_COUNT > 0",
            join_table="APP.MISSING_ALT_ATTRIBUTE_TRACKER",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Alt Text Over X Characters",
            tab="Images",
            description="Alt text over threshold (tracker).",
            sql_where="j.BAD_SRC_COUNT > 0",
            join_table="APP.ALT_TEXT_OVER_X_CHARACTERS_TRACKER",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Background Images",
            tab="Images",
            description="Background images (TODO: requires CSS/image data).",
        ),
        FilterDef(
            name="Incorrectly Sized Images",
            tab="Images",
            description="Incorrectly sized images (TODO: requires image data).",
        ),
        FilterDef(
            name="Missing Size Attributes",
            tab="Images",
            description="Missing width/height attributes (tracker).",
            sql_where="j.BAD_SRC_COUNT > 0",
            join_table="APP.MISSING_SIZE_ATTRIBUTES",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_image_filters()
