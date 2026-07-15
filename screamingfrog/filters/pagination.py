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
    target_join = (
        "FROM APP.LINKS l "
        "JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID "
    )
    non_200_target = (
        "EXISTS (SELECT 1 " + target_join +
        "LEFT JOIN APP.URLS u ON u.ENCODED_URL = d.ENCODED_URL "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND l.LINK_TYPE IN (8, 10) "
        "AND (u.RESPONSE_CODE IS NULL OR u.RESPONSE_CODE NOT BETWEEN 200 AND 299))"
    )
    unlinked_target = (
        "EXISTS (SELECT 1 " + target_join +
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND l.LINK_TYPE IN (8, 10) "
        "AND NOT EXISTS (SELECT 1 FROM APP.LINKS hl "
        "WHERE hl.DST_ID = l.DST_ID AND hl.LINK_TYPE = 1))"
    )
    non_indexable_target = (
        "EXISTS (SELECT 1 " + target_join +
        "JOIN APP.URLS u ON u.ENCODED_URL = d.ENCODED_URL "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND l.LINK_TYPE IN (8, 10) AND " + _non_indexable_sql("u") + ")"
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
            description="Pagination URLs with a non-2xx response.",
            sql_where=non_200_target,
        ),
        FilterDef(
            name="Unlinked Pagination URLs",
            tab="Pagination",
            description="Pagination targets without regular hyperlink inlinks.",
            sql_where=unlinked_target,
        ),
        FilterDef(
            name="Non-Indexable",
            tab="Pagination",
            description="Pagination targets blocked by robots directives.",
            sql_where=non_indexable_target,
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


def _non_indexable_sql(alias: str) -> str:
    robots_names = "'robots', 'googlebot', 'bingbot', 'yandex', 'baiduspider', 'slurp'"
    directives = []
    for prefix in ("", "_JS"):
        for index in range(1, 21):
            directives.append(
                f"(LOWER({alias}.META_NAME{prefix}_{index}) IN ({robots_names}) "
                f"AND LOWER({alias}.META_CONTENT{prefix}_{index}) LIKE '%noindex%')"
            )
    directives.extend(
        f"LOWER({alias}.X_ROBOT_TAG_{index}) LIKE '%noindex%'"
        for index in range(1, 21)
    )
    return (
        "(LOWER(CAST(" + alias + ".BLOCKED_BY_ROBOTS_TXT AS VARCHAR(10))) "
        "IN ('1', 'true') OR " + " OR ".join(directives) + ")"
    )


register_pagination_filters()
