from __future__ import annotations

import json
from pathlib import Path


def _mapping() -> dict[str, list[dict]]:
    return json.loads(Path("schemas/mapping.json").read_text(encoding="utf-8"))


def _entry(tab: str, csv_column: str) -> dict:
    for entry in _mapping()[tab]:
        if entry.get("csv_column") == csv_column:
            return entry
    raise AssertionError(f"Missing mapping for {tab} -> {csv_column}")


def test_content_language_tabs_map_language_code() -> None:
    grammar = _entry("content_grammar_errors.csv", "Language")
    spelling = _entry("content_spelling_errors.csv", "Language")

    assert grammar == {
        "csv_column": "Language",
        "db_column": "LANGUAGE_CODE",
        "db_table": "APP.LANGUAGE_ERROR",
    }
    assert spelling == {
        "csv_column": "Language",
        "db_column": "LANGUAGE_CODE",
        "db_table": "APP.LANGUAGE_ERROR",
    }


def test_alt_text_length_tab_uses_links_expression() -> None:
    entry = _entry("images_with_alt_text_over_x_characters.csv", "Length")

    assert entry["db_table"] == "APP.LINKS"
    assert entry["db_expression"] == (
        "CASE WHEN APP.LINKS.ALT_TEXT IS NULL THEN NULL "
        "ELSE LENGTH(APP.LINKS.ALT_TEXT) END"
    )


def test_pagespeed_report_savings_map_to_pagespeed_api_columns() -> None:
    expected = {
        ("efficiently_encode_images_report.csv", "Potential Savings (ms)"): "EFFICIENTLY_ENCODE_IMAGES_MS",
        ("eliminate_render_blocking_resources_report.csv", "Potential Savings (ms)"): "ELIMINATE_RENDER_BLOCKING_RESOURCES",
        ("enable_text_compression_report.csv", "Potential Savings (Bytes)"): "TEXT_COMPRESSION_SIZE",
        ("minify_css_report.csv", "Potential Savings (Bytes)"): "MINIFY_CSS_SIZE",
        ("minify_javascript_report.csv", "Potential Savings (Bytes)"): "MINIFY_JAVASCRIPT_SIZE",
        ("serve_images_in_next_gen_formats_report.csv", "Potential Savings (Bytes)"): "NEXT_GEN_IMAGES_SIZE",
        ("reduce_unused_css_report.csv", "Potential Savings (Bytes)"): "REMOVE_UNUSED_CSS_SIZE",
        ("reduce_unused_javascript_report.csv", "Potential Savings (Bytes)"): "REMOVE_UNUSED_JAVASCRIPT_SIZE",
        ("defer_offscreen_images_report.csv", "Potential Savings (Bytes)"): "DEFER_OFFSCREEN_IMAGES_SIZE",
        ("use_video_formats_for_animated_content_report.csv", "Potential Savings (Bytes)"): "VIDEO_FORMAT_SIZE",
    }

    for (tab, csv_column), db_column in expected.items():
        entry = _entry(tab, csv_column)
        assert entry == {
            "csv_column": csv_column,
            "db_column": db_column,
            "db_table": "APP.PAGE_SPEED_API",
        }
