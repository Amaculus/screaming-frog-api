from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_pagination_filters() -> None:
    has_next = (
        "EXISTS (SELECT 1 FROM APP.LINKS l "
        "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 10)"
    )
    has_prev = (
        "EXISTS (SELECT 1 FROM APP.LINKS l "
        "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 8)"
    )
    has_pagination = f"({has_next} OR {has_prev})"
    has_multiple = (
        "(SELECT COUNT(*) FROM APP.LINKS l "
        "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE IN (8, 10)) > 1"
    )
    loop_expr = (
        "EXISTS (SELECT 1 FROM APP.LINKS l "
        "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND d.ENCODED_URL = s.ENCODED_URL AND l.LINK_TYPE IN (8, 10))"
    )

    filters = [
        FilterDef(name="All", tab="Pagination", description="All pagination entries."),
        FilterDef(
            name="Contains Pagination",
            tab="Pagination",
            description="Contains rel=next/prev pagination links.",
            sql_where=has_pagination,
        ),
        FilterDef(
            name="First Page",
            tab="Pagination",
            description="First page in pagination series (has next, no prev).",
            sql_where=f"{has_next} AND NOT {has_prev}",
        ),
        FilterDef(
            name="Paginated 2+ Pages",
            tab="Pagination",
            description="Pagination with 2+ pages (rel next/prev).",
            sql_where=has_pagination,
        ),
        FilterDef(
            name="Pagination URL Not in Anchor Tag",
            tab="Pagination",
            description="Pagination URL not in anchor tag (pending link map).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_PAGINATION_PENDING_LINK",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
        FilterDef(
            name="Non-200 Pagination URLs",
            tab="Pagination",
            description="Pagination URLs with non-200 response (TODO: DB columns).",
        ),
        FilterDef(
            name="Unlinked Pagination URLs",
            tab="Pagination",
            description="Unlinked pagination URLs (TODO: DB columns).",
        ),
        FilterDef(
            name="Non-Indexable",
            tab="Pagination",
            description="Non-indexable pagination URLs (TODO: DB columns).",
        ),
        FilterDef(
            name="Multiple Pagination URLs",
            tab="Pagination",
            description="Multiple pagination URLs.",
            sql_where=has_multiple,
        ),
        FilterDef(
            name="Pagination Loop",
            tab="Pagination",
            description="Pagination loop detected (next/prev points to self).",
            sql_where=loop_expr,
        ),
        FilterDef(
            name="Sequence Error",
            tab="Pagination",
            description="Pagination sequence error (multimap).",
            sql_where="j.MULTIMAP_KEY IS NOT NULL",
            join_table="APP.MULTIMAP_PAGINATION_SEQUENCE_ERROR",
            join_on="APP.URLS.ENCODED_URL = j.MULTIMAP_KEY",
            join_type="INNER",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_pagination_filters()
