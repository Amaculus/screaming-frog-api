from __future__ import annotations

from screamingfrog.filters.registry import FilterDef, register_filter


def register_internal_filters() -> None:
    filters = [
        FilterDef(name="All", tab="Internal", description="All internal URLs."),
        FilterDef(
            name="HTML",
            tab="Internal",
            description="Internal URLs with HTML content type.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="JavaScript",
            tab="Internal",
            description="Internal URLs with JavaScript content type.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="CSS",
            tab="Internal",
            description="Internal URLs with CSS content type.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="Images",
            tab="Internal",
            description="Internal URLs with image content types.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="Plugins",
            tab="Internal",
            description="Internal URLs with plugin content types.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="Media",
            tab="Internal",
            description="Internal URLs with media content types.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="Fonts",
            tab="Internal",
            description="Internal URLs with font content types.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="XML",
            tab="Internal",
            description="Internal URLs with XML content type.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="PDF",
            tab="Internal",
            description="Internal URLs with PDF content type.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="Other",
            tab="Internal",
            description="Internal URLs with other content types.",
            columns=["Content Type"],
        ),
        FilterDef(
            name="Unknown",
            tab="Internal",
            description="Internal URLs with unknown content types.",
            columns=["Content Type"],
        ),
    ]
    for filt in filters:
        register_filter(filt)


register_internal_filters()
