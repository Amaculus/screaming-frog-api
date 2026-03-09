from __future__ import annotations

import pytest

from screamingfrog.backends.derby_backend import _strip_default_port


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://example.com:443/path", "https://example.com/path"),
        ("http://example.com:80/path", "http://example.com/path"),
        ("https://example.com:444/path", "https://example.com:444/path"),
        ("https://example.com:abc/path", "https://example.com:abc/path"),
        ("https://user:pass@example.com:443/path", "https://user:pass@example.com/path"),
        ("https://[2001:db8::1]:443/path", "https://[2001:db8::1]/path"),
    ],
)
def test_strip_default_port(raw: str, expected: str) -> None:
    assert _strip_default_port(raw) == expected
