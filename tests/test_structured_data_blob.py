"""Decoding SF's SERIALISED_STRUCTURED_DATA blob.

Screaming Frog writes this column as a Java ObjectOutputStream: one
NTriplesDocumentFormat enum per document, then block data holding a length and
the gzipped N-Triples. Java cuts block data into records of at most 1024 bytes
and writes a record header between them, inside the gzip bytes. A reader that
scans for the gzip magic and inflates the raw bytes decodes only a stream that
fits in one record; Yoast/RankMath JSON-LD graphs almost never do.

The builder below mirrors java.io.ObjectOutputStream's framing, so these
fixtures have the shape SF writes rather than a simplified one.
"""

from __future__ import annotations

import gzip
import random
import sqlite3
import struct

from screamingfrog.backends.derby_backend import (
    DerbyBackend,
    _decode_structured_data_blob,
    _extract_structured_data_types,
)

_FORMAT_CLASS = "uk.co.screamingfrog.seospider.structured.data.raw.NTriplesDocumentFormat"
_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def _utf(value: str) -> bytes:
    data = value.encode("utf-8")
    return struct.pack(">H", len(data)) + data


def _block_records(data: bytes) -> bytes:
    """Frame block data the way ObjectOutputStream does: 1024-byte records."""
    out = bytearray()
    for start in range(0, len(data), 1024):
        chunk = data[start : start + 1024]
        if len(chunk) <= 255:
            out += b"\x77" + bytes([len(chunk)]) + chunk
        else:
            out += b"\x7a" + struct.pack(">I", len(chunk)) + chunk
    return bytes(out)


def _ntriples(types: list[str], filler_triples: int = 0, seed: int = 7) -> bytes:
    """N-Triples naming ``types``, plus incompressible filler to grow the gzip."""
    rng = random.Random(seed)
    lines = [
        f"_:b{index} {_RDF_TYPE} <https://schema.org/{name}> ."
        for index, name in enumerate(types)
    ]
    for index in range(filler_triples):
        token = "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(40))
        lines.append(f'_:f{index} <https://schema.org/name> "{token}" .')
    return ("\n".join(lines) + "\n").encode("utf-8")


def _java_blob(documents: list[tuple[str, bytes]]) -> bytes:
    """A blob shaped like SF's: header block, then (enum, framed gzip) per document.

    The first enum carries the full class descriptors; later enums refer back to
    the class descriptor by handle and name their constant with a new string,
    exactly as the stream on a real crawl does.
    """
    out = bytearray(b"\xac\xed\x00\x05")
    out += _block_records(struct.pack(">qBiq", 1, 1, len(documents), 1))
    for index, (format_name, text) in enumerate(documents):
        if index == 0:
            out += b"\x7e\x72" + _utf(_FORMAT_CLASS) + b"\x00" * 8 + b"\x12\x00\x00\x78"
            out += b"\x72" + _utf("java.lang.Enum") + b"\x00" * 8 + b"\x12\x00\x00\x78\x70"
        else:
            out += b"\x7e\x71" + struct.pack(">I", 0x7E0000)
        out += b"\x74" + _utf(format_name)
        compressed = gzip.compress(text)
        out += _block_records(struct.pack(">i", len(compressed)) + compressed + b"\x00" * 8)
    return bytes(out)


def test_a_gzip_stream_that_crosses_a_record_boundary_decodes() -> None:
    jsonld = _ntriples(["Organization", "WebSite", "BreadcrumbList"], filler_triples=60)
    microdata = _ntriples(["WPHeader"], filler_triples=40, seed=11)
    blob = _java_blob([("JSONLD", jsonld), ("MICRODATA", microdata)])
    # Precondition: the JSON-LD gzip really spans two records, or this test
    # proves nothing about the defect it exists for.
    assert len(gzip.compress(jsonld)) + 4 > 1024

    blocks, failures = _decode_structured_data_blob(blob)

    assert failures == 0
    assert [block["format"] for block in blocks] == ["JSONLD", "MICRODATA"]
    assert _extract_structured_data_types(blocks[0]["text"])[:3] == [
        "Organization",
        "WebSite",
        "BreadcrumbList",
    ]
    assert _extract_structured_data_types(blocks[1]["text"]) == ["WPHeader"]


def test_a_repeated_format_written_as_a_reference_keeps_its_name() -> None:
    blob = bytearray(_java_blob([("JSONLD", _ntriples(["Article"]))]))
    # A second JSON-LD document: the enum constant is written as a back
    # reference to the constant's handle (base + 2: two class descriptors
    # precede it), not as a new TC_ENUM.
    blob += b"\x71" + struct.pack(">I", 0x7E0002)
    compressed = gzip.compress(_ntriples(["Product"]))
    blob += _block_records(struct.pack(">i", len(compressed)) + compressed)

    blocks, failures = _decode_structured_data_blob(bytes(blob))

    assert failures == 0
    assert [(block["format"], _extract_structured_data_types(block["text"])) for block in blocks] == [
        ("JSONLD", ["Article"]),
        ("JSONLD", ["Product"]),
    ]


def test_a_truncated_stream_is_counted_not_dropped() -> None:
    blob = _java_blob([("JSONLD", _ntriples(["Organization"], filler_triples=60))])
    truncated = blob[:900]

    blocks, failures = _decode_structured_data_blob(truncated)

    assert blocks == []
    assert failures == 1


def test_an_unknown_stream_token_is_counted_not_dropped() -> None:
    blob = _java_blob([("JSONLD", _ntriples(["Organization"]))]) + b"\x73"  # TC_OBJECT

    blocks, failures = _decode_structured_data_blob(blob)

    assert blocks == []
    assert failures == 1


def test_a_plain_gzip_blob_still_decodes() -> None:
    blocks, failures = _decode_structured_data_blob(b"JSONLD" + gzip.compress(b"hello"))

    assert failures == 0
    assert blocks == [{"format": "JSONLD", "text": "hello"}]


def test_the_summary_tab_reports_decode_errors_per_url() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute("ATTACH DATABASE ':memory:' AS APP")
    conn.execute(
        "CREATE TABLE APP.URLS ("
        "ENCODED_URL TEXT PRIMARY KEY, SERIALISED_STRUCTURED_DATA BLOB, PARSE_ERROR_MSG TEXT)"
    )
    conn.execute(
        "CREATE TABLE APP.URL_INSPECTION ("
        "ENCODED_URL TEXT PRIMARY KEY, RICH_RESULTS_TYPES TEXT, "
        "RICH_RESULTS_TYPE_ERRORS INTEGER, RICH_RESULTS_TYPE_WARNINGS INTEGER)"
    )
    good = _java_blob([("JSONLD", _ntriples(["Organization", "WebSite"], filler_triples=60))])
    conn.executemany(
        "INSERT INTO APP.URLS (ENCODED_URL, SERIALISED_STRUCTURED_DATA, PARSE_ERROR_MSG) "
        "VALUES (?, ?, ?)",
        [
            ("https://example.com/", good, None),
            ("https://example.com/cut/", good[:900], None),
            ("https://example.com/none/", None, None),
        ],
    )
    backend = object.__new__(DerbyBackend)
    backend._conn = conn
    columns = [
        {"csv_column": "Address", "db_column": "ENCODED_URL", "db_table": "APP.URLS"},
        {"csv_column": "Total Types"},
        {"csv_column": "Decode Errors"},
    ]
    backend._mapping = {
        "structured_data_all.csv": columns,
        "structured_data_missing.csv": columns,
    }

    rows = {row["Address"]: row for row in backend.get_tab("structured_data_all.csv")}
    missing = [row["Address"] for row in backend.get_tab("structured_data_missing.csv")]

    assert rows["https://example.com/"]["Total Types"] == 2
    assert rows["https://example.com/"]["Decode Errors"] == 0
    assert rows["https://example.com/cut/"]["Total Types"] == 0
    assert rows["https://example.com/cut/"]["Decode Errors"] == 1
    assert rows["https://example.com/none/"]["Decode Errors"] == 0
    # An undecodable blob is not a page without structured data.
    assert missing == ["https://example.com/none/"]
    conn.close()
