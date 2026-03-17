from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_response_code_filters() -> None:
    filters = [
        FilterDef(
            name="All",
            tab="Response Codes",
            description="All URLs with a response code.",
        ),
        FilterDef(
            name="Blocked by Robots.txt",
            tab="Response Codes",
            description="URLs blocked by robots.txt.",
            sql_where="BLOCKED_BY_ROBOTS_TXT = 1",
            columns=["Blocked by Robots.txt"],
        ),
        FilterDef(
            name="Blocked Resource",
            tab="Response Codes",
            description="Resource URLs blocked by robots.txt (TODO: verify).",
            sql_where="BLOCKED_BY_ROBOTS_TXT = 1 AND LOADED_AS_A_RESOURCE = 1",
            columns=["Blocked by Robots.txt"],
        ),
        FilterDef(
            name="No Response",
            tab="Response Codes",
            description="No response received.",
            sql_where="RESPONSE_CODE IS NULL OR RESPONSE_CODE = 0",
            columns=["Status Code"],
        ),
        FilterDef(
            name="Success (2xx)",
            tab="Response Codes",
            description="HTTP 2xx responses.",
            sql_where="RESPONSE_CODE BETWEEN 200 AND 299",
            columns=["Status Code"],
        ),
        FilterDef(
            name="Redirection (3xx)",
            tab="Response Codes",
            description="HTTP 3xx responses.",
            sql_where="RESPONSE_CODE BETWEEN 300 AND 399",
            columns=["Status Code"],
        ),
        FilterDef(
            name="Redirection (JavaScript)",
            tab="Response Codes",
            description="JavaScript redirects (TODO: DB flag).",
        ),
        FilterDef(
            name="Redirection (Meta Refresh)",
            tab="Response Codes",
            description="Meta refresh redirects.",
            sql_where="NUM_METAREFRESH > 0",
            columns=["Meta Refresh"],
        ),
        FilterDef(
            name="Client Error (4xx)",
            tab="Response Codes",
            description="HTTP 4xx responses.",
            sql_where="RESPONSE_CODE BETWEEN 400 AND 499",
            columns=["Status Code"],
        ),
        FilterDef(
            name="Server Error (5xx)",
            tab="Response Codes",
            description="HTTP 5xx responses.",
            sql_where="RESPONSE_CODE BETWEEN 500 AND 599",
            columns=["Status Code"],
        ),
        # Internal subset
        FilterDef(
            name="Internal All",
            tab="Response Codes",
            description="Internal URLs with any response code.",
            sql_where="IS_INTERNAL = 1",
        ),
        FilterDef(
            name="Internal Blocked by Robots.txt",
            tab="Response Codes",
            description="Internal URLs blocked by robots.txt.",
            sql_where="IS_INTERNAL = 1 AND BLOCKED_BY_ROBOTS_TXT = 1",
        ),
        FilterDef(
            name="Internal Blocked Resource",
            tab="Response Codes",
            description="Internal resources blocked by robots.txt (TODO: verify).",
            sql_where="IS_INTERNAL = 1 AND BLOCKED_BY_ROBOTS_TXT = 1 AND LOADED_AS_A_RESOURCE = 1",
        ),
        FilterDef(
            name="Internal No Response",
            tab="Response Codes",
            description="Internal URLs with no response.",
            sql_where="IS_INTERNAL = 1 AND (RESPONSE_CODE IS NULL OR RESPONSE_CODE = 0)",
        ),
        FilterDef(
            name="Internal Success (2xx)",
            tab="Response Codes",
            description="Internal URLs with HTTP 2xx responses.",
            sql_where="IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 200 AND 299",
        ),
        FilterDef(
            name="Internal Redirection (3xx)",
            tab="Response Codes",
            description="Internal URLs with HTTP 3xx responses.",
            sql_where="IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 300 AND 399",
        ),
        FilterDef(
            name="Internal Redirection (JavaScript)",
            tab="Response Codes",
            description="Internal JS redirects (TODO: DB flag).",
        ),
        FilterDef(
            name="Internal Redirection (Meta Refresh)",
            tab="Response Codes",
            description="Internal meta refresh redirects.",
            sql_where="IS_INTERNAL = 1 AND NUM_METAREFRESH > 0",
        ),
        FilterDef(
            name="Internal Redirect Chain",
            tab="Response Codes",
            description="Internal redirect chains (multiple redirect hops).",
            sql_where="IS_INTERNAL = 1 AND IS_REDIRECT = 1 AND REDIRECT_COUNT > 0",
        ),
        FilterDef(
            name="Internal Redirect Loop",
            tab="Response Codes",
            description="Internal redirect loops (TODO: DB flag).",
        ),
        FilterDef(
            name="Internal Client Error (4xx)",
            tab="Response Codes",
            description="Internal URLs with HTTP 4xx responses.",
            sql_where="IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 400 AND 499",
        ),
        FilterDef(
            name="Internal Server Error (5xx)",
            tab="Response Codes",
            description="Internal URLs with HTTP 5xx responses.",
            sql_where="IS_INTERNAL = 1 AND RESPONSE_CODE BETWEEN 500 AND 599",
        ),
        # External subset
        FilterDef(
            name="External All",
            tab="Response Codes",
            description="External URLs with any response code.",
            sql_where="IS_INTERNAL = 0",
        ),
        FilterDef(
            name="External Blocked by Robots.txt",
            tab="Response Codes",
            description="External URLs blocked by robots.txt.",
            sql_where="IS_INTERNAL = 0 AND BLOCKED_BY_ROBOTS_TXT = 1",
        ),
        FilterDef(
            name="External Blocked Resource",
            tab="Response Codes",
            description="External resources blocked by robots.txt (TODO: verify).",
            sql_where="IS_INTERNAL = 0 AND BLOCKED_BY_ROBOTS_TXT = 1 AND LOADED_AS_A_RESOURCE = 1",
        ),
        FilterDef(
            name="External No Response",
            tab="Response Codes",
            description="External URLs with no response.",
            sql_where="IS_INTERNAL = 0 AND (RESPONSE_CODE IS NULL OR RESPONSE_CODE = 0)",
        ),
        FilterDef(
            name="External Success (2xx)",
            tab="Response Codes",
            description="External URLs with HTTP 2xx responses.",
            sql_where="IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 200 AND 299",
        ),
        FilterDef(
            name="External Redirection (3xx)",
            tab="Response Codes",
            description="External URLs with HTTP 3xx responses.",
            sql_where="IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 300 AND 399",
        ),
        FilterDef(
            name="External Redirection (JavaScript)",
            tab="Response Codes",
            description="External JS redirects (TODO: DB flag).",
        ),
        FilterDef(
            name="External Redirection (Meta Refresh)",
            tab="Response Codes",
            description="External meta refresh redirects.",
            sql_where="IS_INTERNAL = 0 AND NUM_METAREFRESH > 0",
        ),
        FilterDef(
            name="External Client Error (4xx)",
            tab="Response Codes",
            description="External URLs with HTTP 4xx responses.",
            sql_where="IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 400 AND 499",
        ),
        FilterDef(
            name="External Server Error (5xx)",
            tab="Response Codes",
            description="External URLs with HTTP 5xx responses.",
            sql_where="IS_INTERNAL = 0 AND RESPONSE_CODE BETWEEN 500 AND 599",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_response_code_filters()
