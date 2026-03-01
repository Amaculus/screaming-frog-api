from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


_META_NAME_COLUMNS = [f"META_NAME_{i}" for i in range(1, 21)]
_META_CONTENT_COLUMNS = [f"META_CONTENT_{i}" for i in range(1, 21)]
_META_NAME_JS_COLUMNS = [f"META_NAME_JS_{i}" for i in range(1, 21)]
_META_CONTENT_JS_COLUMNS = [f"META_CONTENT_JS_{i}" for i in range(1, 21)]
_X_ROBOT_COLUMNS = [f"X_ROBOT_TAG_{i}" for i in range(1, 21)]
_ROBOT_NAMES = ("robots", "googlebot", "bingbot", "yandex", "baiduspider", "slurp")


def _meta_robot_clauses(token_expr: str) -> list[str]:
    names = "', '".join(_ROBOT_NAMES)
    clauses: list[str] = []
    for name_col, content_col in zip(_META_NAME_COLUMNS, _META_CONTENT_COLUMNS):
        clauses.append(
            f"(LOWER({name_col}) IN ('{names}') AND {token_expr.format(col=content_col)})"
        )
    for name_col, content_col in zip(_META_NAME_JS_COLUMNS, _META_CONTENT_JS_COLUMNS):
        clauses.append(
            f"(LOWER({name_col}) IN ('{names}') AND {token_expr.format(col=content_col)})"
        )
    return clauses


def _x_robot_clauses(token_expr: str) -> list[str]:
    return [token_expr.format(col=col) for col in _X_ROBOT_COLUMNS]


def _robots_or_xrobots(token_expr: str) -> str:
    clauses = _meta_robot_clauses(token_expr) + _x_robot_clauses(token_expr)
    return "(" + " OR ".join(clauses) + ")"


def _token_like(token: str) -> str:
    return "LOWER({col}) LIKE '%" + token.lower() + "%'"


def _token_like_not(token: str, not_token: str) -> str:
    return (
        "LOWER({col}) LIKE '%" + token.lower() + "%' AND "
        "LOWER({col}) NOT LIKE '%" + not_token.lower() + "%'"
    )


def register_directive_filters() -> None:
    filters = [
        FilterDef(name="All", tab="Directives", description="All directive entries."),
        FilterDef(
            name="Index",
            tab="Directives",
            description="Index directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like_not("index", "noindex")),
        ),
        FilterDef(
            name="Noindex",
            tab="Directives",
            description="Noindex directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("noindex")),
        ),
        FilterDef(
            name="Follow",
            tab="Directives",
            description="Follow directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like_not("follow", "nofollow")),
        ),
        FilterDef(
            name="Nofollow",
            tab="Directives",
            description="Nofollow directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("nofollow")),
        ),
        FilterDef(
            name="None",
            tab="Directives",
            description="None directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("none")),
        ),
        FilterDef(
            name="NoArchive",
            tab="Directives",
            description="Noarchive directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("noarchive")),
        ),
        FilterDef(
            name="NoSnippet",
            tab="Directives",
            description="Nosnippet directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("nosnippet")),
        ),
        FilterDef(
            name="Max-Snippet",
            tab="Directives",
            description="Max-snippet directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("max-snippet")),
        ),
        FilterDef(
            name="Max-Image-Preview",
            tab="Directives",
            description="Max-image-preview directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("max-image-preview")),
        ),
        FilterDef(
            name="Max-Video-Preview",
            tab="Directives",
            description="Max-video-preview directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("max-video-preview")),
        ),
        FilterDef(
            name="NoODP",
            tab="Directives",
            description="NoODP directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("noodp")),
        ),
        FilterDef(
            name="NoYDIR",
            tab="Directives",
            description="NoYDIR directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("noydir")),
        ),
        FilterDef(
            name="NoImageIndex",
            tab="Directives",
            description="Noimageindex directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("noimageindex")),
        ),
        FilterDef(
            name="NoTranslate",
            tab="Directives",
            description="Notranslate directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("notranslate")),
        ),
        FilterDef(
            name="Unavailable_After",
            tab="Directives",
            description="Unavailable_after directive (meta robots/X-Robots-Tag).",
            sql_where=_robots_or_xrobots(_token_like("unavailable_after")),
        ),
        FilterDef(
            name="Refresh",
            tab="Directives",
            description="Meta refresh directive.",
            sql_where="NUM_METAREFRESH > 0",
        ),
        FilterDef(
            name="Outside <head>",
            tab="Directives",
            description="Meta robots outside head.",
            sql_where="j.META_ROBOTS_OUTSIDE_HEAD = 1",
            join_table="APP.HTML_VALIDATION_DATA",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_directive_filters()
