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


def test_internal_page_from_data_can_reuse_existing_dict() -> None:
    row = {"ENCODED_URL": "https://example.com/", "RESPONSE_CODE": 200}

    page = InternalPage.from_data(row, copy_data=False)

    assert page.data is row
    assert page.address == "https://example.com/"
    assert page.status_code == 200
