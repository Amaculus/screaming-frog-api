from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_structured_data_filters() -> None:
    has_structured_data = (
        "SERIALISED_STRUCTURED_DATA IS NOT NULL "
        "AND LENGTH(SERIALISED_STRUCTURED_DATA) > 0"
    )
    filters = [
        FilterDef(name="All", tab="Structured Data", description="All structured data entries."),
        FilterDef(
            name="Contains Structured Data",
            tab="Structured Data",
            description="URLs with structured data.",
            sql_where=has_structured_data,
        ),
        FilterDef(
            name="Missing",
            tab="Structured Data",
            description="Missing structured data.",
            sql_where=(
                "SERIALISED_STRUCTURED_DATA IS NULL "
                "OR LENGTH(SERIALISED_STRUCTURED_DATA) = 0"
            ),
        ),
        FilterDef(
            name="Validation Errors",
            tab="Structured Data",
            description="Structured data validation errors (TODO: DB columns).",
        ),
        FilterDef(
            name="Validation Warnings",
            tab="Structured Data",
            description="Structured data validation warnings (TODO: DB columns).",
        ),
        FilterDef(
            name="Rich Result Validation Errors",
            tab="Structured Data",
            description="Rich result validation errors (URL Inspection).",
            sql_where="j.RICH_RESULTS_TYPE_ERRORS IS NOT NULL AND j.RICH_RESULTS_TYPE_ERRORS <> ''",
            join_table="APP.URL_INSPECTION",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Rich Result Validation Warnings",
            tab="Structured Data",
            description="Rich result validation warnings (URL Inspection).",
            sql_where="j.RICH_RESULTS_TYPE_WARNINGS IS NOT NULL AND j.RICH_RESULTS_TYPE_WARNINGS <> ''",
            join_table="APP.URL_INSPECTION",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
        FilterDef(
            name="Parse Errors",
            tab="Structured Data",
            description="Structured data parse errors.",
            sql_where="PARSE_ERROR_MSG IS NOT NULL AND PARSE_ERROR_MSG <> ''",
        ),
        FilterDef(
            name="Microdata URLs",
            tab="Structured Data",
            description="URLs with Microdata.",
            sql_where=has_structured_data,
            blob_column="SERIALISED_STRUCTURED_DATA",
            blob_pattern=b"MICRODATA",
        ),
        FilterDef(
            name="JSON-LD URLs",
            tab="Structured Data",
            description="URLs with JSON-LD.",
            sql_where=has_structured_data,
            blob_column="SERIALISED_STRUCTURED_DATA",
            blob_pattern=b"JSONLD",
        ),
        FilterDef(
            name="RDFa URLs",
            tab="Structured Data",
            description="URLs with RDFa.",
            sql_where=has_structured_data,
            blob_column="SERIALISED_STRUCTURED_DATA",
            blob_pattern=b"RDFA",
        ),
        FilterDef(
            name="Rich Result Feature Detected",
            tab="Structured Data",
            description="Rich result feature detected (URL Inspection).",
            sql_where="j.RICH_RESULTS_TYPES IS NOT NULL AND j.RICH_RESULTS_TYPES <> ''",
            join_table="APP.URL_INSPECTION",
            join_on="APP.URLS.ENCODED_URL = j.ENCODED_URL",
            join_type="INNER",
        ),
    ]

    for filt in filters:
        register_filter(filt)


register_structured_data_filters()
