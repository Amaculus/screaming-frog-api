from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_hreflang_filters() -> None:
    filters = [
        FilterDef(name="All", tab="Hreflang", description="All hreflang entries."),
        FilterDef(
            name="Contains hreflang",
            tab="Hreflang",
            description="Contains hreflang annotations.",
            sql_where=(
                "EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 13)"
            ),
        ),
        FilterDef(
            name="Non-200 hreflang URLs",
            tab="Hreflang",
            description="Hreflang URLs with non-200 responses (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_HREF_LANG_NON_200_LINK",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
        FilterDef(
            name="Unlinked hreflang URLs",
            tab="Hreflang",
            description="Unlinked hreflang URLs (TODO: DB columns).",
        ),
        FilterDef(
            name="Missing Return Links",
            tab="Hreflang",
            description="Missing return links (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_HREF_LANG_MISSING_CONFIRMATION",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
        FilterDef(
            name="Inconsistent Language & Region Return Links",
            tab="Hreflang",
            description="Inconsistent language/region return links (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_HREF_LANG_INCONSISTENT_LANGUAGE_CONFIRMATION",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
        FilterDef(
            name="Non-Canonical Return Links",
            tab="Hreflang",
            description="Non-canonical return links (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_HREF_LANG_CANONICAL_CONFIRMATION",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
        FilterDef(
            name="Noindex Return Links",
            tab="Hreflang",
            description="Noindex return links (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_HREF_LANG_NO_INDEX_CONFIRMATION",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
        FilterDef(
            name="Incorrect Language & Region Codes",
            tab="Hreflang",
            description="Incorrect language/region codes (TODO: DB columns).",
        ),
        FilterDef(
            name="Multiple Entries",
            tab="Hreflang",
            description="Multiple hreflang entries.",
            sql_where=(
                "(SELECT COUNT(*) FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 13) > 1"
            ),
        ),
        FilterDef(
            name="Missing Self Reference",
            tab="Hreflang",
            description="Missing self-referencing hreflang.",
            sql_where=(
                "NOT EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
                "AND l.LINK_TYPE = 13 AND d.ENCODED_URL = s.ENCODED_URL)"
            ),
        ),
        FilterDef(
            name="Not Using Canonical",
            tab="Hreflang",
            description="Hreflang points to URLs that are canonicalised elsewhere.",
            sql_where=(
                "EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
                "JOIN APP.URLS u ON u.ENCODED_URL = d.ENCODED_URL "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
                "AND l.LINK_TYPE = 13 AND u.IS_CANONICALISED = 1)"
            ),
        ),
        FilterDef(
            name="Missing X-Default",
            tab="Hreflang",
            description="Missing x-default hreflang.",
            sql_where=(
                "NOT EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
                "AND l.LINK_TYPE = 13 AND LOWER(l.HREF_LANG) = 'x-default')"
            ),
        ),
        FilterDef(
            name="Missing",
            tab="Hreflang",
            description="Missing hreflang.",
            sql_where=(
                "NOT EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 13)"
            ),
        ),
        FilterDef(
            name="Outside <head>",
            tab="Hreflang",
            description="Hreflang outside <head>.",
            sql_where="j.HREFLANG_OUTSIDE_HEAD = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_hreflang_filters()
