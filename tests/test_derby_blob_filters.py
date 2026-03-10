from __future__ import annotations

from screamingfrog.backends.derby_backend import _blob_contains, _resolve_blob_checks
from screamingfrog.filters.registry import FilterDef


class DummyBlob:
    def __init__(self, payload: bytes):
        self.payload = payload

    def length(self) -> int:
        return len(self.payload)

    def getBytes(self, start: int, size: int) -> bytes:
        begin = max(start - 1, 0)
        end = begin + size
        return self.payload[begin:end]


def test_blob_contains_handles_java_blob_and_is_case_insensitive() -> None:
    blob = DummyBlob(b"\xac\xedfooJsonLdBar")
    assert _blob_contains(blob, b"JSONLD")
    assert not _blob_contains(blob, b"MICRODATA")


def test_blob_contains_handles_bytes_payload() -> None:
    assert _blob_contains(b"prefixRDFApostfix", b"rdfa")
    assert not _blob_contains(b"", b"RDFA")


def test_resolve_blob_checks_reads_filter_blob_fields() -> None:
    checks = _resolve_blob_checks(
        [
            FilterDef(name="All", tab="Structured Data"),
            FilterDef(
                name="JSON-LD URLs",
                tab="Structured Data",
                blob_column="SERIALISED_STRUCTURED_DATA",
                blob_pattern=b"JSONLD",
            ),
        ]
    )
    assert checks == [("SERIALISED_STRUCTURED_DATA", b"JSONLD")]
