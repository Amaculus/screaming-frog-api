from __future__ import annotations

from screamingfrog.models import InternalPage


def test_internal_page_from_db_row_supports_derby_keys() -> None:
    page = InternalPage.from_db_row(
        ["ENCODED_URL", "RESPONSE_CODE", "ID"],
        ("https://example.com/", 404, 7),
    )

    assert page.address == "https://example.com/"
    assert page.status_code == 404
    assert page.id == 7
