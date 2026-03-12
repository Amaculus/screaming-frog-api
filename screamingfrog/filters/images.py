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
            description="Images loaded via CSS background-image (link type 23).",
            sql_where=(
                "CONTENT_TYPE LIKE 'image/%'"
                " AND EXISTS ("
                "SELECT 1 FROM APP.LINKS l"
                " JOIN APP.UNIQUE_URLS u ON l.DST_ID = u.ID"
                " WHERE u.ENCODED_URL = APP.URLS.ENCODED_URL"
                " AND l.LINK_TYPE = 23"
                ")"
            ),
        ),
        FilterDef(
            name="Incorrectly Sized Images",
            tab="Images",
            description="Images rendered at dimensions that differ from their intrinsic size.",
            sql_where=(
                "CONTENT_TYPE LIKE 'image/%'"
                " AND IMAGE_WIDTH > 0"
                " AND EXISTS ("
                "SELECT 1 FROM APP.LINKS l"
                " JOIN APP.UNIQUE_URLS u ON l.DST_ID = u.ID"
                " WHERE u.ENCODED_URL = APP.URLS.ENCODED_URL"
                " AND l.IMAGE_DISPLAY_WIDTH > 0"
                " AND (l.IMAGE_DISPLAY_WIDTH <> APP.URLS.IMAGE_WIDTH"
                " OR l.IMAGE_DISPLAY_HEIGHT <> APP.URLS.IMAGE_HEIGHT)"
                ")"
            ),
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
