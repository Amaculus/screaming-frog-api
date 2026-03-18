from __future__ import annotations

import json
from pathlib import Path

from scripts.suggest_mappings import generate_mapping_nulls_content


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


def test_additional_pagespeed_report_mappings_use_verified_columns() -> None:
    expected = {
        ("preload_key_requests_report.csv", "Potential Savings (ms)"): "PRELOAD",
        ("properly_size_images_report.csv", "Potential Savings (Bytes)"): "PROPERLY_SIZE_IMAGES_SIZE",
    }

    for (tab, csv_column), db_column in expected.items():
        entry = _entry(tab, csv_column)
        assert entry == {
            "csv_column": csv_column,
            "db_column": db_column,
            "db_table": "APP.PAGE_SPEED_API",
        }


def test_mobile_pagespeed_tabs_map_request_status_expression() -> None:
    expected_expr = (
        "CASE WHEN SF_REQUEST_ERROR_KEY IS NOT NULL AND SF_REQUEST_ERROR_KEY <> '' "
        "THEN SF_REQUEST_ERROR_KEY ELSE 'Success' END"
    )
    tabs = [
        "mobile_content_not_sized_correctly.csv",
        "mobile_illegible_font_size.csv",
        "mobile_target_size.csv",
        "mobile_viewport_not_set.csv",
    ]

    for tab in tabs:
        entry = _entry(tab, "PSI Request Status")
        assert entry == {
            "csv_column": "PSI Request Status",
            "db_expression": expected_expr,
            "db_table": "APP.PAGE_SPEED_API",
        }


def test_content_and_internal_tabs_map_readability_and_near_duplicate_fields() -> None:
    avg_tabs = [
        "content_all.csv",
        "content_readability_difficult.csv",
        "content_readability_very_difficult.csv",
        "internal_all.csv",
        "internal_css.csv",
        "internal_fonts.csv",
        "internal_html.csv",
        "internal_images.csv",
        "internal_javascript.csv",
        "internal_media.csv",
        "internal_other.csv",
        "internal_pdf.csv",
        "internal_plugins.csv",
        "internal_unknown.csv",
        "internal_xml.csv",
    ]

    for tab in avg_tabs:
        assert _entry(tab, "Average Words Per Sentence") == {
            "csv_column": "Average Words Per Sentence",
            "db_column": "AVG_WORDS_PER_SENTENCE",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Flesch Reading Ease Score") == {
            "csv_column": "Flesch Reading Ease Score",
            "db_column": "READABILITY_SCORE",
            "db_table": "APP.URLS",
        }

    near_dup_tabs = [
        "content_all.csv",
        "internal_all.csv",
        "internal_css.csv",
        "internal_fonts.csv",
        "internal_html.csv",
        "internal_images.csv",
        "internal_javascript.csv",
        "internal_media.csv",
        "internal_other.csv",
        "internal_pdf.csv",
        "internal_plugins.csv",
        "internal_unknown.csv",
        "internal_xml.csv",
    ]
    closest_expr = (
        "(SELECT nd.CLOSEST_MATCH_PERCENTAGE FROM APP.NEAR_DUPLICATE nd "
        "WHERE nd.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )
    count_expr = (
        "(SELECT nd.NUMBER_OF_NEAR_DUPLICATES FROM APP.NEAR_DUPLICATE nd "
        "WHERE nd.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )

    for tab in near_dup_tabs:
        assert _entry(tab, "Closest Near Duplicate Match") == {
            "csv_column": "Closest Near Duplicate Match",
            "db_expression": closest_expr,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "No. Near Duplicates") == {
            "csv_column": "No. Near Duplicates",
            "db_expression": count_expr,
            "db_table": "APP.URLS",
        }


def test_hash_and_language_rollouts_cover_internal_content_and_url_tabs() -> None:
    hash_tabs = [
        "content_all.csv",
        "internal_all.csv",
        "internal_css.csv",
        "internal_fonts.csv",
        "internal_html.csv",
        "internal_images.csv",
        "internal_javascript.csv",
        "internal_media.csv",
        "internal_other.csv",
        "internal_pdf.csv",
        "internal_plugins.csv",
        "internal_unknown.csv",
        "internal_xml.csv",
        "url_all.csv",
        "url_broken_bookmark.csv",
        "url_contains_space.csv",
        "url_ga_tracking_parameters.csv",
        "url_internal_search.csv",
        "url_multiple_slashes.csv",
        "url_non_ascii_characters.csv",
        "url_over_115_characters.csv",
        "url_parameters.csv",
        "url_repetitive_path.csv",
        "url_underscores.csv",
        "url_uppercase.csv",
    ]
    for tab in hash_tabs:
        assert _entry(tab, "Hash") == {
            "csv_column": "Hash",
            "db_column": "MD5SUM",
            "db_table": "APP.URLS",
        }

    language_tabs = [
        "content_all.csv",
        "internal_all.csv",
        "internal_css.csv",
        "internal_fonts.csv",
        "internal_html.csv",
        "internal_images.csv",
        "internal_javascript.csv",
        "internal_media.csv",
        "internal_other.csv",
        "internal_pdf.csv",
        "internal_plugins.csv",
        "internal_unknown.csv",
        "internal_xml.csv",
    ]
    for tab in language_tabs:
        assert _entry(tab, "Language") == {
            "csv_column": "Language",
            "db_column": "LANGUAGE_CODE",
            "db_table": "APP.LANGUAGE_ERROR",
        }


def test_content_all_maps_total_language_errors_expression() -> None:
    assert _entry("content_all.csv", "Total Language Errors") == {
        "csv_column": "Total Language Errors",
        "db_expression": (
            "(SELECT COALESCE(le.SPELLING_ERRORS, 0) + "
            "COALESCE(le.GRAMMAR_ERRORS, 0) FROM APP.LANGUAGE_ERROR le "
            "WHERE le.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.URLS",
    }


def test_internal_tabs_map_url_timestamp_and_http_metadata_fields() -> None:
    internal_tabs = [
        "internal_all.csv",
        "internal_css.csv",
        "internal_fonts.csv",
        "internal_html.csv",
        "internal_images.csv",
        "internal_javascript.csv",
        "internal_media.csv",
        "internal_other.csv",
        "internal_pdf.csv",
        "internal_plugins.csv",
        "internal_unknown.csv",
        "internal_xml.csv",
    ]

    for tab in internal_tabs:
        assert _entry(tab, "Response Time") == {
            "csv_column": "Response Time",
            "db_column": "RESPONSE_TIME_MS",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Last Modified") == {
            "csv_column": "Last Modified",
            "db_column": "LAST_MODIFIED_DATE",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "URL Encoded Address") == {
            "csv_column": "URL Encoded Address",
            "db_column": "ENCODED_URL",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Crawl Timestamp") == {
            "csv_column": "Crawl Timestamp",
            "db_column": "TIMESTAMP",
            "db_table": "APP.URLS",
        }


def test_url_family_tabs_map_encoded_address_and_js_blocked_resources_maps_response_time() -> None:
    url_tabs = [
        "url_all.csv",
        "url_broken_bookmark.csv",
        "url_contains_space.csv",
        "url_ga_tracking_parameters.csv",
        "url_internal_search.csv",
        "url_multiple_slashes.csv",
        "url_non_ascii_characters.csv",
        "url_over_115_characters.csv",
        "url_parameters.csv",
        "url_repetitive_path.csv",
        "url_underscores.csv",
        "url_uppercase.csv",
    ]

    for tab in url_tabs:
        assert _entry(tab, "URL Encoded Address") == {
            "csv_column": "URL Encoded Address",
            "db_column": "ENCODED_URL",
            "db_table": "APP.URLS",
        }

    assert _entry("javascript_pages_with_blocked_resources.csv", "Response Time") == {
        "csv_column": "Response Time",
        "db_column": "RESPONSE_TIME_MS",
        "db_table": "APP.URLS",
    }


def test_url_family_tabs_map_length_from_encoded_address() -> None:
    url_tabs = [
        "url_all.csv",
        "url_broken_bookmark.csv",
        "url_contains_space.csv",
        "url_ga_tracking_parameters.csv",
        "url_internal_search.csv",
        "url_multiple_slashes.csv",
        "url_non_ascii_characters.csv",
        "url_over_115_characters.csv",
        "url_parameters.csv",
        "url_repetitive_path.csv",
        "url_underscores.csv",
        "url_uppercase.csv",
    ]
    expected = {
        "csv_column": "Length",
        "db_expression": (
            "CASE WHEN APP.URLS.ENCODED_URL IS NULL THEN NULL "
            "ELSE LENGTH(APP.URLS.ENCODED_URL) END"
        ),
        "db_table": "APP.URLS",
    }

    for tab in url_tabs:
        assert _entry(tab, "Length") == expected


def test_ai_all_maps_crawl_timestamp_from_urls_table() -> None:
    assert _entry("ai_all.csv", "Crawl Timestamp") == {
        "csv_column": "Crawl Timestamp",
        "db_column": "TIMESTAMP",
        "db_table": "APP.URLS",
    }


def test_form_action_link_tabs_map_to_destination_url() -> None:
    expected = {
        "csv_column": "Form Action Link",
        "db_expression": (
            "(SELECT d.ENCODED_URL FROM APP.UNIQUE_URLS d "
            "WHERE d.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.LINKS",
    }

    assert _entry("form_url_insecure.csv", "Form Action Link") == expected
    assert _entry("all_inlinks.csv", "Form Action Link") == expected


def test_generate_mapping_nulls_only_counts_literal_null_placeholders() -> None:
    content = generate_mapping_nulls_content(
        {
            "example.csv": [
                {"csv_column": "Literal NULL", "db_expression": "NULL"},
                {
                    "csv_column": "Expression NULL",
                    "db_expression": "CASE WHEN foo IS NULL THEN NULL ELSE 1 END",
                },
            ]
        },
        {"example.csv": []},
    )

    assert "- example.csv: Literal NULL" in content
    assert "Expression NULL" not in content


def test_readability_label_rollout_uses_flesch_score_bands() -> None:
    readability_tabs = [
        "content_all.csv",
        "content_readability_difficult.csv",
        "content_readability_very_difficult.csv",
        "internal_all.csv",
        "internal_css.csv",
        "internal_fonts.csv",
        "internal_html.csv",
        "internal_images.csv",
        "internal_javascript.csv",
        "internal_media.csv",
        "internal_other.csv",
        "internal_pdf.csv",
        "internal_plugins.csv",
        "internal_unknown.csv",
        "internal_xml.csv",
    ]
    expr = (
        "CASE WHEN READABILITY_SCORE IS NULL THEN NULL "
        "WHEN READABILITY_SCORE >= 90 THEN 'Very Easy' "
        "WHEN READABILITY_SCORE >= 80 THEN 'Easy' "
        "WHEN READABILITY_SCORE >= 70 THEN 'Fairly Easy' "
        "WHEN READABILITY_SCORE >= 60 THEN 'Standard' "
        "WHEN READABILITY_SCORE >= 50 THEN 'Fairly Difficult' "
        "WHEN READABILITY_SCORE >= 30 THEN 'Difficult' "
        "ELSE 'Very Difficult' END"
    )

    for tab in readability_tabs:
        assert _entry(tab, "Readability") == {
            "csv_column": "Readability",
            "db_expression": expr,
            "db_table": "APP.URLS",
        }
