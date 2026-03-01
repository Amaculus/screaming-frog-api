from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_canonical_filters() -> None:
    filters = [
        FilterDef(name="All", tab="Canonicals", description="All canonicals."),
        FilterDef(
            name="Canonicalised",
            tab="Canonicals",
            description="URLs with canonicalised flag.",
            sql_where="IS_CANONICALISED = 1",
        ),
        FilterDef(
            name="Missing",
            tab="Canonicals",
            description="Missing canonical tag.",
            sql_where=(
                "NOT EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 6)"
            ),
        ),
        FilterDef(
            name="Multiple",
            tab="Canonicals",
            description="Multiple canonical tags.",
            sql_where=(
                "(SELECT COUNT(*) FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 6) > 1"
            ),
        ),
        FilterDef(
            name="Multiple Conflicting",
            tab="Canonicals",
            description="Multiple conflicting canonicals.",
            sql_where=(
                "(SELECT COUNT(DISTINCT d.ENCODED_URL) FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 6) > 1"
            ),
        ),
        FilterDef(
            name="Canonical Is Relative",
            tab="Canonicals",
            description="Canonical URL is relative (TODO: DB columns).",
        ),
        FilterDef(
            name="Contains Canonical",
            tab="Canonicals",
            description="Contains canonical tag.",
            sql_where=(
                "EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 6)"
            ),
        ),
        FilterDef(
            name="Contains Fragment URL",
            tab="Canonicals",
            description="Canonical contains fragment URL (HTML validation).",
            sql_where="j.CANONICAL_CONTAINS_FRAGMENT_URL = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
        FilterDef(
            name="Invalid Attribute In Annotation",
            tab="Canonicals",
            description="Canonical has invalid attribute (HTML validation).",
            sql_where="j.CANONICAL_CONTAINS_INVALID_ATTRIBUTE = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
        FilterDef(
            name="Outside <head>",
            tab="Canonicals",
            description="Canonical outside head (HTML validation).",
            sql_where="j.CANONICAL_OUTSIDE_HEAD = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
        FilterDef(
            name="Self Referencing",
            tab="Canonicals",
            description="Self-referencing canonical.",
            sql_where=(
                "EXISTS (SELECT 1 FROM APP.LINKS l "
                "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
                "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
                "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
                "AND l.LINK_TYPE = 6 AND d.ENCODED_URL = s.ENCODED_URL)"
            ),
        ),
        FilterDef(
            name="Non-Indexable Canonical",
            tab="Canonicals",
            description="Non-indexable canonical target (TODO: DB columns).",
        ),
        FilterDef(
            name="Unlinked",
            tab="Canonicals",
            description="Canonicals with unlinked targets (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_CANONICALS_PENDING_LINK",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_canonical_filters()
