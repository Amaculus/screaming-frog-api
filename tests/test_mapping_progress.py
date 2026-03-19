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


def _coalesced_meta_expression(prefix: str, names: tuple[str, ...]) -> str:
    clauses = []
    for i in range(1, 21):
        name_col = f"META_NAME{prefix}_{i}" if prefix else f"META_NAME_{i}"
        content_col = f"META_CONTENT{prefix}_{i}" if prefix else f"META_CONTENT_{i}"
        if len(names) == 1:
            clauses.append(
                f"CASE WHEN LOWER({name_col}) = '{names[0]}' "
                f"THEN NULLIF({content_col}, '') END"
            )
        else:
            joined = ", ".join(f"'{name}'" for name in names)
            clauses.append(
                f"CASE WHEN LOWER({name_col}) IN ({joined}) "
                f"THEN NULLIF({content_col}, '') END"
            )
    return "COALESCE(" + ", ".join(clauses) + ")"


def _directive_occurrence_expression(
    token: str, *, not_token: str | None = None
) -> str:
    robot_names = (
        "'robots', 'googlebot', 'bingbot', 'yandex', 'baiduspider', 'slurp'"
    )
    clauses = []
    for prefix in ("", "_JS"):
        for i in range(1, 21):
            name_col = f"META_NAME{prefix}_{i}"
            content_col = f"META_CONTENT{prefix}_{i}"
            clause = f"LOWER({content_col}) LIKE '%{token.lower()}%'"
            if not_token:
                clause += f" AND LOWER({content_col}) NOT LIKE '%{not_token.lower()}%'"
            clauses.append(
                f"CASE WHEN LOWER({name_col}) IN ({robot_names}) AND {clause} "
                "THEN 1 ELSE 0 END"
            )
    for i in range(1, 21):
        col = f"X_ROBOT_TAG_{i}"
        clause = f"LOWER({col}) LIKE '%{token.lower()}%'"
        if not_token:
            clause += f" AND LOWER({col}) NOT LIKE '%{not_token.lower()}%'"
        clauses.append(f"CASE WHEN {clause} THEN 1 ELSE 0 END")
    return "(" + " + ".join(clauses) + ")"


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
    }

    for (tab, csv_column), db_column in expected.items():
        entry = _entry(tab, csv_column)
        assert entry == {
            "csv_column": csv_column,
            "db_column": db_column,
            "db_table": "APP.PAGE_SPEED_API",
        }


def test_pagespeed_detail_tabs_mark_runtime_rows() -> None:
    detail_tabs = {
        "avoid_excessive_dom_size_report.csv": [
            "URL",
            "Statistic",
            "Selector",
            "Snippet",
            "Value",
        ],
        "avoid_large_layout_shifts_report.csv": [
            "Source Page",
            "Label",
            "Snippet",
            "CLS Contribution",
        ],
        "avoid_serving_legacy_javascript_to_modern_browsers_report.csv": [
            "Source Page",
            "URL",
            "Size (Bytes)",
            "Potential Savings (Bytes)",
        ],
        "reduce_javascript_execution_time_report.csv": [
            "Source Page",
            "URL",
            "Total CPU Time (ms)",
            "Script Evaluation",
            "Script Parse",
        ],
        "serve_static_assets_with_an_efficient_cache_policy_report.csv": [
            "Source Page",
            "URL",
            "Cache TTL (ms)",
            "Size (Bytes)",
        ],
        "illegible_font_size_report.csv": [
            "Source Page",
            "Font Size",
            "% of Page Text",
            "Selector",
            "URL",
        ],
        "image_elements_do_not_have_explicit_width_&_height_report.csv": [
            "Source Page",
            "URL",
            "Label",
            "Snippet",
        ],
        "defer_offscreen_images_report.csv": [
            "Source Page",
            "Image URL",
            "Size (Bytes)",
            "Potential Savings (Bytes)",
        ],
        "use_video_formats_for_animated_content_report.csv": [
            "Source Page",
            "Image URL",
            "Size (Bytes)",
            "Potential Savings (Bytes)",
        ],
    }
    for tab, columns in detail_tabs.items():
        for column in columns:
            assert _entry(tab, column) == {
                "csv_column": column,
                "db_expression": "NULL",
                "db_table": "APP.PAGE_SPEED_API",
                "runtime_extract": {
                    "type": "pagespeed_detail",
                    "tab": tab,
                    "field": column,
                },
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


def test_directive_tabs_map_occurrences_from_meta_and_xrobots() -> None:
    expected = {
        "directives_follow.csv": _directive_occurrence_expression(
            "follow",
            not_token="nofollow",
        ),
        "directives_index.csv": _directive_occurrence_expression(
            "index",
            not_token="noindex",
        ),
        "directives_maximagepreview.csv": _directive_occurrence_expression(
            "max-image-preview"
        ),
        "directives_maxsnippet.csv": _directive_occurrence_expression(
            "max-snippet"
        ),
        "directives_maxvideopreview.csv": _directive_occurrence_expression(
            "max-video-preview"
        ),
        "directives_noarchive.csv": _directive_occurrence_expression(
            "noarchive"
        ),
        "directives_nofollow.csv": _directive_occurrence_expression(
            "nofollow"
        ),
        "directives_noimageindex.csv": _directive_occurrence_expression(
            "noimageindex"
        ),
        "directives_noindex.csv": _directive_occurrence_expression(
            "noindex"
        ),
        "directives_none.csv": _directive_occurrence_expression("none"),
        "directives_noodp.csv": _directive_occurrence_expression("noodp"),
        "directives_nosnippet.csv": _directive_occurrence_expression(
            "nosnippet"
        ),
        "directives_notranslate.csv": _directive_occurrence_expression(
            "notranslate"
        ),
        "directives_noydir.csv": _directive_occurrence_expression("noydir"),
        "directives_refresh.csv": "COALESCE(NUM_METAREFRESH,0)",
        "directives_unavailable_after.csv": _directive_occurrence_expression(
            "unavailable_after"
        ),
    }

    for tab, expression in expected.items():
        assert _entry(tab, "Occurrences") == {
            "csv_column": "Occurrences",
            "db_expression": expression,
            "db_table": "APP.URLS",
        }


def test_chain_tabs_mark_runtime_supported_fields_as_chain_runtime_extract() -> None:
    for tab in [
        "redirects.csv",
        "redirect_chains.csv",
        "redirect_and_canonical_chains.csv",
        "canonical_chains.csv",
    ]:
        assert _entry(tab, "Final Address") == {
            "csv_column": "Final Address",
            "db_table": "APP.URLS",
            "runtime_extract": {"type": "chain_row", "field": "Final Address"},
        }
        assert _entry(tab, "Redirect Type 1") == {
            "csv_column": "Redirect Type 1",
            "db_table": "APP.URLS",
            "runtime_extract": {"type": "chain_row", "field": "Redirect Type 1"},
        }

    for tab in [
        "redirects.csv",
        "redirect_chains.csv",
        "redirect_and_canonical_chains.csv",
    ]:
        assert _entry(tab, "Temp Redirect in Chain") == {
            "csv_column": "Temp Redirect in Chain",
            "db_table": "APP.URLS",
            "runtime_extract": {
                "type": "chain_row",
                "field": "Temp Redirect in Chain",
            },
        }


def test_accessibility_special_tabs_mark_runtime_supported_columns() -> None:
    for tab in [
        "all_violations.csv",
        "all_incomplete.csv",
        "best_practice_all_violations.csv",
        "wcag_2_0_a_all_violations.csv",
    ]:
        assert _entry(tab, "Issue") == {
            "csv_column": "Issue",
            "db_expression": "NULL",
            "db_table": "APP.URLS",
            "runtime_extract": {
                "type": "accessibility_detail",
                "field": "Issue",
            },
        }
        assert _entry(tab, "Help URL") == {
            "csv_column": "Help URL",
            "db_expression": "NULL",
            "db_table": "APP.URLS",
            "runtime_extract": {
                "type": "accessibility_detail",
                "field": "Help URL",
            },
        }

    assert _entry("accessibility_violations_summary.csv", "Issue") == {
        "csv_column": "Issue",
        "db_expression": "NULL",
        "db_table": "APP.URLS",
        "runtime_extract": {
            "type": "accessibility_summary",
            "field": "Issue",
        },
    }
    assert _entry("accessibility_violations_summary.csv", "% URLs in Violation") == {
        "csv_column": "% URLs in Violation",
        "db_expression": "NULL",
        "db_table": "APP.URLS",
        "runtime_extract": {
            "type": "accessibility_summary",
            "field": "% URLs in Violation",
        },
    }


def test_pagespeed_summary_tabs_mark_runtime_supported_columns() -> None:
    assert _entry("pagespeed_opportunities_summary.csv", "Opportunity") == {
        "csv_column": "Opportunity",
        "db_expression": "NULL",
        "db_table": "APP.URLS",
        "runtime_extract": {
            "type": "pagespeed_opportunity_summary",
            "field": "Opportunity",
        },
    }
    for tab in ["css_coverage_summary.csv", "js_coverage_summary.csv"]:
        assert _entry(tab, "Resource") == {
            "csv_column": "Resource",
            "db_expression": "NULL",
            "db_table": "APP.URLS",
            "runtime_extract": {
            "type": "pagespeed_coverage_summary",
            "field": "Resource",
        },
    }


def test_google_rich_results_tabs_mark_runtime_supported_columns() -> None:
    assert _entry("google_rich_results_features_report.csv", "Google FAQ") == {
        "csv_column": "Google FAQ",
        "db_expression": "NULL",
        "db_table": "APP.URLS",
        "runtime_extract": {
            "type": "google_rich_results_features_report",
            "field": "Google FAQ",
        },
    }
    assert _entry(
        "google_rich_results_features_summary_report.csv", "Rich Results Feature"
    ) == {
        "csv_column": "Rich Results Feature",
        "db_expression": "NULL",
        "db_table": "APP.LANGUAGE_ERROR_COUNTS",
        "runtime_extract": {
            "type": "google_rich_results_features_summary",
            "field": "Rich Results Feature",
        },
    }


def test_image_report_tabs_map_dimension_fields_from_links_and_target_urls() -> None:
    real_dims_expr = (
        "(SELECT CASE WHEN u.IMAGE_WIDTH > 0 AND u.IMAGE_HEIGHT > 0 "
        "THEN CAST(u.IMAGE_WIDTH AS VARCHAR(10)) || 'x' || CAST(u.IMAGE_HEIGHT AS VARCHAR(10)) END "
        "FROM APP.URLS u JOIN APP.UNIQUE_URLS d ON d.ID = APP.LINKS.DST_ID "
        "WHERE u.ENCODED_URL = d.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )
    attr_dims_expr = (
        "CASE WHEN APP.LINKS.IMAGE_WIDTH_ATTRIBUTE > 0 AND APP.LINKS.IMAGE_HEIGHT_ATTRIBUTE > 0 "
        "THEN CAST(APP.LINKS.IMAGE_WIDTH_ATTRIBUTE AS VARCHAR(10)) || 'x' || "
        "CAST(APP.LINKS.IMAGE_HEIGHT_ATTRIBUTE AS VARCHAR(10)) END"
    )
    display_dims_expr = (
        "CASE WHEN APP.LINKS.IMAGE_DISPLAY_WIDTH > 0 AND APP.LINKS.IMAGE_DISPLAY_HEIGHT > 0 "
        "THEN CAST(APP.LINKS.IMAGE_DISPLAY_WIDTH AS VARCHAR(10)) || 'x' || "
        "CAST(APP.LINKS.IMAGE_DISPLAY_HEIGHT AS VARCHAR(10)) END"
    )
    length_expr = "CASE WHEN APP.LINKS.ALT_TEXT IS NULL THEN NULL ELSE LENGTH(APP.LINKS.ALT_TEXT) END"

    for tab in ["incorrectly_sized_images.csv", "missing_size_attributes.csv"]:
        assert _entry(tab, "Length") == {
            "csv_column": "Length",
            "db_expression": length_expr,
            "db_table": "APP.LINKS",
        }
        assert _entry(tab, "Real Dimensions") == {
            "csv_column": "Real Dimensions",
            "db_expression": real_dims_expr,
            "db_table": "APP.LINKS",
        }
        assert _entry(tab, "Dimensions in Attributes") == {
            "csv_column": "Dimensions in Attributes",
            "db_expression": attr_dims_expr,
            "db_table": "APP.LINKS",
        }
        assert _entry(tab, "Display Dimensions") == {
            "csv_column": "Display Dimensions",
            "db_expression": display_dims_expr,
            "db_table": "APP.LINKS",
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


def test_javascript_content_tabs_map_word_count_fields() -> None:
    assert _entry("javascript_contains_javascript_content.csv", "HTML Word Count") == {
        "csv_column": "HTML Word Count",
        "db_column": "WORD_COUNT",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_contains_javascript_content.csv", "Rendered HTML Word Count") == {
        "csv_column": "Rendered HTML Word Count",
        "db_expression": "COALESCE(WORD_COUNT, 0) + COALESCE(WORD_COUNT_JS, 0)",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_contains_javascript_content.csv", "Word Count Change") == {
        "csv_column": "Word Count Change",
        "db_expression": "COALESCE(WORD_COUNT_JS, 0)",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_contains_javascript_content.csv", "JS Word Count %") == {
        "csv_column": "JS Word Count %",
        "db_expression": (
            "CAST((100.0 * COALESCE(WORD_COUNT_JS, 0)) / "
            "NULLIF(COALESCE(WORD_COUNT, 0) + COALESCE(WORD_COUNT_JS, 0), 0) "
            "AS DECIMAL(12, 3))"
        ),
        "db_table": "APP.URLS",
    }


def test_javascript_title_and_h1_tabs_map_original_and_rendered_fields() -> None:
    for tab in [
        "javascript_page_title_updated_by_javascript.csv",
        "javascript_page_title_only_in_rendered_html.csv",
    ]:
        assert _entry(tab, "HTML Title") == {
            "csv_column": "HTML Title",
            "db_column": "TITLE_1",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Rendered HTML Title") == {
            "csv_column": "Rendered HTML Title",
            "db_column": "TITLE_JS_1",
            "db_table": "APP.URLS",
        }

    for tab in [
        "javascript_h1_updated_by_javascript.csv",
        "javascript_h1_only_in_rendered_html.csv",
    ]:
        assert _entry(tab, "HTML H1") == {
            "csv_column": "HTML H1",
            "db_column": "H1_1",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Rendered HTML H1") == {
            "csv_column": "Rendered HTML H1",
            "db_column": "H1_JS_1",
            "db_table": "APP.URLS",
        }


def test_javascript_meta_description_and_robots_tabs_map_original_and_rendered_fields() -> None:
    original_desc = _coalesced_meta_expression("", ("description",))
    rendered_desc = _coalesced_meta_expression("_JS", ("description",))
    original_robots = _coalesced_meta_expression(
        "",
        ("robots", "googlebot", "bingbot", "yandex", "baiduspider", "slurp"),
    )
    rendered_robots = _coalesced_meta_expression(
        "_JS",
        ("robots", "googlebot", "bingbot", "yandex", "baiduspider", "slurp"),
    )

    for tab in [
        "javascript_meta_description_updated_by_javascript.csv",
        "javascript_meta_description_only_in_rendered_html.csv",
    ]:
        assert _entry(tab, "HTML Meta Description") == {
            "csv_column": "HTML Meta Description",
            "db_expression": original_desc,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Rendered HTML Meta Description") == {
            "csv_column": "Rendered HTML Meta Description",
            "db_expression": rendered_desc,
            "db_table": "APP.URLS",
        }

    for tab in [
        "javascript_noindex_only_in_original_html.csv",
        "javascript_nofollow_only_in_original_html.csv",
    ]:
        assert _entry(tab, "HTML Meta Robots 1") == {
            "csv_column": "HTML Meta Robots 1",
            "db_expression": original_robots,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Rendered HTML Meta Robots 1") == {
            "csv_column": "Rendered HTML Meta Robots 1",
            "db_expression": rendered_robots,
            "db_table": "APP.URLS",
        }


def test_javascript_all_maps_current_rendered_and_console_fields() -> None:
    assert _entry("javascript_all.csv", "HTML Word Count") == {
        "csv_column": "HTML Word Count",
        "db_column": "WORD_COUNT",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "Rendered HTML Word Count") == {
        "csv_column": "Rendered HTML Word Count",
        "db_expression": "COALESCE(WORD_COUNT, 0) + COALESCE(WORD_COUNT_JS, 0)",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "Word Count Change") == {
        "csv_column": "Word Count Change",
        "db_expression": "COALESCE(WORD_COUNT_JS, 0)",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "JS Word Count %") == {
        "csv_column": "JS Word Count %",
        "db_expression": (
            "CAST((100.0 * COALESCE(WORD_COUNT_JS, 0)) / "
            "NULLIF(COALESCE(WORD_COUNT, 0) + COALESCE(WORD_COUNT_JS, 0), 0) "
            "AS DECIMAL(12, 3))"
        ),
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "HTML Title") == {
        "csv_column": "HTML Title",
        "db_column": "TITLE_1",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "Rendered HTML Title") == {
        "csv_column": "Rendered HTML Title",
        "db_column": "TITLE_JS_1",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "HTML H1") == {
        "csv_column": "HTML H1",
        "db_column": "H1_1",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "Rendered HTML H1") == {
        "csv_column": "Rendered HTML H1",
        "db_column": "H1_JS_1",
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "HTML Meta Description") == {
        "csv_column": "HTML Meta Description",
        "db_expression": _coalesced_meta_expression("", ("description",)),
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "Rendered HTML Meta Description") == {
        "csv_column": "Rendered HTML Meta Description",
        "db_expression": _coalesced_meta_expression("_JS", ("description",)),
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "HTML Meta Robots 1") == {
        "csv_column": "HTML Meta Robots 1",
        "db_expression": _coalesced_meta_expression(
            "",
            ("robots", "googlebot", "bingbot", "yandex", "baiduspider", "slurp"),
        ),
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "Rendered HTML Meta Robots 1") == {
        "csv_column": "Rendered HTML Meta Robots 1",
        "db_expression": _coalesced_meta_expression(
            "_JS",
            ("robots", "googlebot", "bingbot", "yandex", "baiduspider", "slurp"),
        ),
        "db_table": "APP.URLS",
    }
    assert _entry("javascript_all.csv", "JS Error") == {
        "csv_column": "JS Error",
        "db_column": "NUM_ERRORS",
        "db_table": "APP.CHROME_CONSOLE_DATA",
    }
    assert _entry("javascript_all.csv", "JS Warning") == {
        "csv_column": "JS Warning",
        "db_column": "NUM_WARNINGS",
        "db_table": "APP.CHROME_CONSOLE_DATA",
    }
    assert _entry("javascript_all.csv", "JS Info") == {
        "csv_column": "JS Info",
        "db_column": "NUM_INFO",
        "db_table": "APP.CHROME_CONSOLE_DATA",
    }
    assert _entry("javascript_all.csv", "JS Debug") == {
        "csv_column": "JS Debug",
        "db_column": "NUM_DEBUG",
        "db_table": "APP.CHROME_CONSOLE_DATA",
    }
    assert _entry("javascript_all.csv", "JS Issue") == {
        "csv_column": "JS Issue",
        "db_column": "NUM_ISSUES",
        "db_table": "APP.CHROME_CONSOLE_DATA",
    }


def test_javascript_canonical_tabs_split_html_and_rendered_canonicals() -> None:
    html_expr = (
        "(SELECT d.ENCODED_URL FROM APP.LINKS l JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND l.LINK_TYPE = 6 AND l.ORIGIN = 1 FETCH FIRST 1 ROWS ONLY)"
    )
    rendered_expr = (
        "(SELECT d.ENCODED_URL FROM APP.LINKS l JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND l.LINK_TYPE = 6 AND l.ORIGIN = 3 FETCH FIRST 1 ROWS ONLY)"
    )
    for tab in [
        "javascript_all.csv",
        "javascript_canonical_mismatch.csv",
        "javascript_canonical_only_in_rendered_html.csv",
    ]:
        assert _entry(tab, "HTML Canonical") == {
            "csv_column": "HTML Canonical",
            "db_expression": html_expr,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Rendered HTML Canonical") == {
            "csv_column": "Rendered HTML Canonical",
            "db_expression": rendered_expr,
            "db_table": "APP.URLS",
        }


def test_javascript_issue_tabs_map_console_counts_from_chrome_console_table() -> None:
    for tab in [
        "javascript_pages_with_chrome_issues.csv",
        "javascript_pages_with_javascript_errors.csv",
        "javascript_pages_with_javascript_warnings.csv",
    ]:
        assert _entry(tab, "JS Error") == {
            "csv_column": "JS Error",
            "db_column": "NUM_ERRORS",
            "db_table": "APP.CHROME_CONSOLE_DATA",
        }
        assert _entry(tab, "JS Warning") == {
            "csv_column": "JS Warning",
            "db_column": "NUM_WARNINGS",
            "db_table": "APP.CHROME_CONSOLE_DATA",
        }
        assert _entry(tab, "JS Issue") == {
            "csv_column": "JS Issue",
            "db_column": "NUM_ISSUES",
            "db_table": "APP.CHROME_CONSOLE_DATA",
        }


def test_url_inspection_rich_results_marks_runtime_fields() -> None:
    for column in [
        "Rich Results",
        "Rich Results Type",
        "Severity",
        "Item Name",
        "Rich Results Issue Type",
    ]:
        assert _entry("url_inspection_rich_results.csv", column) == {
            "csv_column": column,
            "db_expression": "NULL",
            "db_table": "APP.URL_INSPECTION",
            "runtime_extract": {
                "type": "url_inspection_rich_results",
                "field": column,
            },
        }


def test_url_inspection_tab_mappings_cover_sitemaps_and_referrers() -> None:
    assert _entry("url_inspection_sitemaps.csv", "Inspected URL") == {
        "csv_column": "Inspected URL",
        "db_column": "SRC",
        "db_table": "APP.SITEMAP_RESULTS",
    }
    assert _entry("url_inspection_sitemaps.csv", "Sitemap") == {
        "csv_column": "Sitemap",
        "db_column": "SITEMAP_URL",
        "db_table": "APP.SITEMAP_RESULTS",
    }
    assert _entry("url_inspection_referring_pages.csv", "Referring Page") == {
        "csv_column": "Referring Page",
        "db_expression": (
            "(SELECT s.ENCODED_URL FROM APP.UNIQUE_URLS s "
            "WHERE s.ID = APP.LINKS.SRC_ID FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.LINKS",
    }
    assert _entry("url_inspection_referring_pages.csv", "Inspected URL") == {
        "csv_column": "Inspected URL",
        "db_expression": (
            "(SELECT d.ENCODED_URL FROM APP.UNIQUE_URLS d "
            "WHERE d.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.LINKS",
    }


def test_hreflang_tabs_map_second_html_variant_from_links() -> None:
    lang_expr = (
        "(SELECT l.HREF_LANG FROM APP.LINKS l JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL AND l.LINK_TYPE = 13 "
        "ORDER BY l.DST_ID OFFSET 1 ROW FETCH NEXT 1 ROW ONLY)"
    )
    url_expr = (
        "(SELECT d.ENCODED_URL FROM APP.LINKS l JOIN APP.UNIQUE_URLS s ON l.SRC_ID = s.ID "
        "JOIN APP.UNIQUE_URLS d ON l.DST_ID = d.ID WHERE s.ENCODED_URL = APP.URLS.ENCODED_URL "
        "AND l.LINK_TYPE = 13 ORDER BY l.DST_ID OFFSET 1 ROW FETCH NEXT 1 ROW ONLY)"
    )
    for tab in ["hreflang_all.csv", "hreflang_contains_hreflang.csv"]:
        assert _entry(tab, "HTML hreflang 2") == {
            "csv_column": "HTML hreflang 2",
            "db_expression": lang_expr,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "HTML hreflang 2 URL") == {
            "csv_column": "HTML hreflang 2 URL",
            "db_expression": url_expr,
            "db_table": "APP.URLS",
        }


def test_hreflang_multimap_tabs_mark_runtime_fields() -> None:
    cases = {
        "hreflang_missing_return_links.csv": [
            "URL Missing Return Link",
            "URL Not Returning Link",
            "Expected Link",
            "hreflang",
        ],
        "hreflang_inconsistent_language_return_links.csv": [
            "URL with Inconsistent Language Return Link",
            "URL Target",
            "URL Returning with Inconsistent Language",
            "Expected Language",
            "Actual Language",
        ],
        "hreflang_non_canonical_return_links.csv": [
            "Non Canonical Return Link URL",
            "Canonical",
        ],
        "hreflang_no_index_return_links.csv": [
            "Noindex URL",
            "Language",
        ],
    }
    for tab, columns in cases.items():
        for column in columns:
            assert _entry(tab, column) == {
                "csv_column": column,
                "db_expression": "NULL",
                "db_table": "APP.URLS",
                "runtime_extract": {
                    "type": "hreflang_multimap",
                    "tab": tab,
                    "field": column,
                },
            }


def test_javascript_ajax_tabs_derive_pretty_and_ugly_urls() -> None:
    for tab in [
        "javascript_all.csv",
        "javascript_uses_old_ajax_crawling_scheme_urls.csv",
        "javascript_uses_old_ajax_crawling_scheme_meta_fragment_tag.csv",
    ]:
        assert _entry(tab, "Pretty URL") == {
            "csv_column": "Pretty URL",
            "db_column": "ENCODED_URL",
            "db_table": "APP.URLS",
            "derived_extract": {
                "type": "ajax_url_variant",
                "variant": "pretty",
                "columns": ["CRAWL_IN_ESCAPED_FRAGMENT_FORM"],
            },
        }
        assert _entry(tab, "Ugly URL") == {
            "csv_column": "Ugly URL",
            "db_column": "ENCODED_URL",
            "db_table": "APP.URLS",
            "derived_extract": {
                "type": "ajax_url_variant",
                "variant": "ugly",
                "columns": ["CRAWL_IN_ESCAPED_FRAGMENT_FORM"],
            },
        }


def test_internal_tabs_derive_amphtml_link_from_original_content() -> None:
    tabs = [
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
    expected = {
        "csv_column": "amphtml Link Element",
        "db_column": "ENCODED_URL",
        "db_table": "APP.URLS",
        "derived_extract": {
            "type": "html_link_element",
            "rel": "amphtml",
            "columns": ["ORIGINAL_CONTENT"],
        },
    }
    for tab in tabs:
        assert _entry(tab, "amphtml Link Element") == expected


def test_internal_tabs_derive_mobile_alternate_link_from_original_content() -> None:
    tabs = [
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
        "mobile_all.csv",
        "mobile_mobile_alternate_link.csv",
    ]
    expected = {
        "csv_column": "Mobile Alternate Link",
        "db_column": "ENCODED_URL",
        "db_table": "APP.URLS",
        "derived_extract": {
            "type": "mobile_alternate_link",
            "columns": ["ORIGINAL_CONTENT"],
        },
    }
    for tab in tabs:
        assert _entry(tab, "Mobile Alternate Link") == expected


def test_http_header_summary_marks_runtime_request_header_names() -> None:
    assert _entry("http_header_summary.csv", "HTTP Request Headers") == {
        "csv_column": "HTTP Request Headers",
        "db_expression": "NULL",
        "db_table": "APP.URLS",
        "runtime_extract": {
            "type": "http_header_summary",
            "field": "HTTP Request Headers",
        },
    }


def test_directives_outside_head_occurrences_sum_html_validation_flags() -> None:
    assert _entry("directives_outside_head.csv", "Occurrences") == {
        "csv_column": "Occurrences",
        "db_expression": (
            "(SELECT (CASE WHEN h.TITLE_OUTSIDE_HEAD THEN 1 ELSE 0 END) + "
            "(CASE WHEN h.META_DESCRIPTION_OUTSIDE_HEAD THEN 1 ELSE 0 END) + "
            "(CASE WHEN h.META_ROBOTS_OUTSIDE_HEAD THEN 1 ELSE 0 END) + "
            "(CASE WHEN h.CANONICAL_OUTSIDE_HEAD THEN 1 ELSE 0 END) + "
            "(CASE WHEN h.HREFLANG_OUTSIDE_HEAD THEN 1 ELSE 0 END) "
            "FROM APP.HTML_VALIDATION_DATA h WHERE h.ENCODED_URL = APP.URLS.ENCODED_URL "
            "FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.URLS",
    }


def test_lorem_ipsum_and_viewport_tabs_map_direct_fields() -> None:
    assert _entry("content_lorem_ipsum_placeholder.csv", "Occurrences") == {
        "csv_column": "Occurrences",
        "db_column": "LOREM_IPSUM_OCCURRENCES",
        "db_table": "APP.URLS",
    }
    assert _entry("viewport_not_set_report.csv", "Viewport Content") == {
        "csv_column": "Viewport Content",
        "db_expression": _coalesced_meta_expression("", ("viewport",)),
        "db_table": "APP.URLS",
    }


def test_semantic_similarity_and_relevance_tabs_map_derby_similarity_tables() -> None:
    closest_expr = (
        "(SELECT cs.CLOSEST_URL FROM APP.COSINE_SIMILARITY cs "
        "WHERE cs.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )
    score_expr = (
        "(SELECT cs.SCORE FROM APP.COSINE_SIMILARITY cs "
        "WHERE cs.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )
    count_expr = (
        "(SELECT cs.SIMILAR_URLS FROM APP.COSINE_SIMILARITY cs "
        "WHERE cs.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )
    relevance_expr = (
        "(SELECT lr.SCORE FROM APP.LOW_RELEVANCE lr "
        "WHERE lr.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )

    for tab in ["content_all.csv", "content_semantically_similar.csv", "semantically_similar_report.csv"]:
        assert _entry(tab, "Closest Semantically Similar Address") == {
            "csv_column": "Closest Semantically Similar Address",
            "db_expression": closest_expr,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Semantic Similarity Score") == {
            "csv_column": "Semantic Similarity Score",
            "db_expression": score_expr,
            "db_table": "APP.URLS",
        }

    for tab in ["content_all.csv", "content_semantically_similar.csv"]:
        assert _entry(tab, "No. Semantically Similar") == {
            "csv_column": "No. Semantically Similar",
            "db_expression": count_expr,
            "db_table": "APP.URLS",
        }

    for tab in ["content_all.csv", "content_low_relevance_content.csv"]:
        assert _entry(tab, "Semantic Relevance Score") == {
            "csv_column": "Semantic Relevance Score",
            "db_expression": relevance_expr,
            "db_table": "APP.URLS",
        }


def test_generate_mapping_nulls_only_counts_literal_null_placeholders() -> None:
    content = generate_mapping_nulls_content(
        {
            "example.csv": [
                {"csv_column": "Literal NULL", "db_expression": "NULL"},
                {
                    "csv_column": "Runtime Backed NULL",
                    "db_expression": "NULL",
                    "runtime_extract": {"type": "chain_row", "field": "Source"},
                },
                {
                    "csv_column": "Expression NULL",
                    "db_expression": "CASE WHEN foo IS NULL THEN NULL ELSE 1 END",
                },
                {
                    "csv_column": "Derived Backed NULL",
                    "db_expression": "NULL",
                    "derived_extract": {"type": "folder_depth", "columns": ["ENCODED_URL"]},
                },
                {
                    "csv_column": "Multi Row Backed NULL",
                    "db_expression": "NULL",
                    "multi_row_extract": {
                        "type": "custom_extraction_match",
                        "source": "encoded_url",
                        "extractor_idx": 0,
                        "match_index": 1,
                        "columns": ["ENCODED_URL"],
                    },
                },
            ]
        },
        {"example.csv": []},
    )

    assert "- example.csv: Literal NULL" in content
    assert "Runtime Backed NULL" not in content
    assert "Expression NULL" not in content
    assert "Derived Backed NULL" not in content
    assert "Multi Row Backed NULL" not in content


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


def test_js_outlink_rollout_maps_direct_url_counts() -> None:
    tabs = [
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
        "javascript_all.csv",
        "javascript_contains_javascript_links.csv",
        "links_all.csv",
        "links_follow_nofollow_internal_inlinks_to_page.csv",
        "links_internal_nofollow_inlinks_only.csv",
        "links_internal_nofollow_outlinks.csv",
        "links_internal_outlinks_with_no_anchor_text.csv",
        "links_nondescriptive_anchor_text_in_internal_outlinks.csv",
        "links_nonindexable_page_inlinks_only.csv",
        "links_outlinks_to_localhost.csv",
        "links_pages_with_high_crawl_depth.csv",
        "links_pages_with_high_external_outlinks.csv",
        "links_pages_with_high_internal_outlinks.csv",
        "links_pages_without_internal_outlinks.csv",
    ]
    js_expr = (
        "COALESCE(NUM_JS_UNIQUE_INTERNAL_OUTLINKS,0) + "
        "COALESCE(NUM_JS_UNIQUE_EXTERNAL_OUTLINKS,0)"
    )

    for tab in tabs:
        assert _entry(tab, "Unique JS Outlinks") == {
            "csv_column": "Unique JS Outlinks",
            "db_expression": js_expr,
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Unique External JS Outlinks") == {
            "csv_column": "Unique External JS Outlinks",
            "db_column": "NUM_JS_UNIQUE_EXTERNAL_OUTLINKS",
            "db_table": "APP.URLS",
        }


def test_hreflang_link_reports_and_webfont_savings_use_direct_columns() -> None:
    for tab in [
        "hreflang_non200_hreflang_urls.csv",
        "hreflang_unlinked_hreflang_urls.csv",
    ]:
        assert _entry(tab, "hreflang") == {
            "csv_column": "hreflang",
            "db_column": "HREF_LANG",
            "db_table": "APP.LINKS",
        }

    assert _entry(
        "ensure_text_remains_visible_during_webfont_load_report.csv",
        "Potential Savings (ms)",
    ) == {
        "csv_column": "Potential Savings (ms)",
        "db_column": "TEXT_VISIBLE_DURING_LOAD",
        "db_table": "APP.PAGE_SPEED_API",
    }


def test_redirect_type_rollout_uses_http_redirect_label_and_internal_carryover() -> None:
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
        "javascript_pages_with_blocked_resources.csv",
    ]
    redirect_url = {
        "csv_column": "Redirect URL",
        "db_column": "ENCODED_URL",
        "derived_extract": {
            "type": "redirect_url",
            "columns": [
                "ENCODED_URL",
                "RESPONSE_CODE",
                "NUM_METAREFRESH",
                "META_FULL_URL_1",
                "META_FULL_URL_2",
                "HTTP_RESPONSE_HEADER_COLLECTION",
            ],
        },
        "db_table": "APP.URLS",
    }
    redirect_type = {
        "csv_column": "Redirect Type",
        "db_expression": (
            "CASE WHEN NUM_METAREFRESH > 0 THEN 'Meta Refresh' "
            "WHEN RESPONSE_CODE BETWEEN 300 AND 399 THEN 'HTTP Redirect' ELSE NULL END"
        ),
        "db_table": "APP.URLS",
    }

    for tab in internal_tabs:
        assert _entry(tab, "Redirect URL") == redirect_url
        assert _entry(tab, "Redirect Type") == redirect_type

    for tab in [
        "response_codes_all.csv",
        "response_codes_internal_all.csv",
        "response_codes_internal_redirection_(3xx).csv",
    ]:
        assert _entry(tab, "Redirect Type") == redirect_type


def test_internal_tabs_map_pagespeed_issue_summary_text() -> None:
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
    expected = {
        "Image Elements Do Not Have Explicit Width & Height": "UNSIZED_IMAGES",
        "Avoid Large Layout Shifts": "LAYOUT_SHIFT_ELEMENTS",
    }

    for tab in internal_tabs:
        for csv_column, db_column in expected.items():
            assert _entry(tab, csv_column) == {
                "csv_column": csv_column,
                "db_expression": (
                    f"(SELECT psi.{db_column} FROM APP.PAGE_SPEED_API psi "
                    "WHERE psi.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
                ),
                "db_table": "APP.URLS",
            }


def test_internal_tabs_map_cookie_counts_via_blob_extract() -> None:
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
        assert _entry(tab, "Cookies") == {
            "csv_column": "Cookies",
            "db_column": "COOKIE_COLLECTION",
            "db_table": "APP.URLS",
            "blob_extract": {"type": "cookie_count"},
        }


def test_internal_tabs_map_folder_depth_via_derived_extract() -> None:
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
        assert _entry(tab, "Folder Depth") == {
            "csv_column": "Folder Depth",
            "db_column": "ENCODED_URL",
            "db_table": "APP.URLS",
            "derived_extract": {"type": "folder_depth", "columns": ["ENCODED_URL"]},
        }


def test_title_and_meta_pixel_widths_use_derived_extracts() -> None:
    title_tabs = [
        "amp_all.csv",
        "amp_contains_disallowed_html.csv",
        "amp_indexable.csv",
        "amp_missing_body_tag.csv",
        "amp_missing_canonical.csv",
        "amp_missing_canonical_to_nonamp.csv",
        "amp_missing_head_tag.csv",
        "amp_missing_html_amp_tag.csv",
        "amp_missing_invalid_amp_boilerplate.csv",
        "amp_missing_invalid_amp_script.csv",
        "amp_missing_invalid_doctype_html_tag.csv",
        "amp_missing_invalid_meta_charset_tag.csv",
        "amp_missing_invalid_meta_viewport_tag.csv",
        "amp_missing_nonamp_return_link.csv",
        "amp_non200_response.csv",
        "amp_nonindexable.csv",
        "amp_nonindexable_canonical.csv",
        "amp_other_validation_errors.csv",
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
        "page_titles_all.csv",
        "page_titles_below_200_pixels.csv",
        "page_titles_below_30_characters.csv",
        "page_titles_duplicate.csv",
        "page_titles_missing.csv",
        "page_titles_multiple.csv",
        "page_titles_outside_head.csv",
        "page_titles_over_561_pixels.csv",
        "page_titles_over_60_characters.csv",
        "page_titles_same_as_h1.csv",
    ]
    meta_tabs = [
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
        "meta_description_all.csv",
        "meta_description_below_400_pixels.csv",
        "meta_description_below_70_characters.csv",
        "meta_description_duplicate.csv",
        "meta_description_missing.csv",
        "meta_description_multiple.csv",
        "meta_description_outside_head.csv",
        "meta_description_over_155_characters.csv",
        "meta_description_over_985_pixels.csv",
    ]

    for tab in title_tabs:
        assert _entry(tab, "Title 1 Pixel Width") == {
            "csv_column": "Title 1 Pixel Width",
            "db_column": "TITLE_1",
            "db_table": "APP.URLS",
            "derived_extract": {
                "type": "pixel_width",
                "profile": "title",
                "columns": ["TITLE_1"],
            },
        }

    for tab in meta_tabs:
        assert _entry(tab, "Meta Description 1 Pixel Width") == {
            "csv_column": "Meta Description 1 Pixel Width",
            "db_column": "META_NAME_1",
            "db_table": "APP.URLS",
            "derived_extract": {
                "type": "meta_description_pixel_width",
                "columns": [
                    *[item for i in range(1, 21) for item in (f"META_NAME_{i}", f"META_CONTENT_{i}")],
                    *[
                        item
                        for i in range(1, 21)
                        for item in (f"META_NAME_JS_{i}", f"META_CONTENT_JS_{i}")
                    ],
                ],
            },
        }


def test_carbon_rating_uses_co2_derived_extract() -> None:
    carbon_tabs = [
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
        "validation_all.csv",
        "validation_body_element_preceding_html.csv",
        "validation_head_not_first_in_html_element.csv",
        "validation_high_carbon_rating.csv",
        "validation_html_document_over_15mb.csv",
        "validation_invalid_html_elements_in_head.csv",
        "validation_missing_body_tag.csv",
        "validation_missing_head_tag.csv",
        "validation_multiple_body_tags.csv",
        "validation_multiple_head_tags.csv",
        "validation_resource_over_15mb.csv",
    ]

    for tab in carbon_tabs:
        assert _entry(tab, "Carbon Rating") == {
            "csv_column": "Carbon Rating",
            "db_column": "CO2",
            "db_table": "APP.URLS",
            "derived_extract": {"type": "carbon_rating", "columns": ["CO2"]},
        }


def test_percent_of_total_maps_from_unique_inlinks_against_internal_html_denominator() -> None:
    tabs = [
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
        "links_all.csv",
        "links_follow_nofollow_internal_inlinks_to_page.csv",
        "links_internal_nofollow_inlinks_only.csv",
        "links_internal_nofollow_outlinks.csv",
        "links_internal_outlinks_with_no_anchor_text.csv",
        "links_nondescriptive_anchor_text_in_internal_outlinks.csv",
        "links_nonindexable_page_inlinks_only.csv",
        "links_outlinks_to_localhost.csv",
        "links_pages_with_high_crawl_depth.csv",
        "links_pages_with_high_external_outlinks.csv",
        "links_pages_with_high_internal_outlinks.csv",
        "links_pages_without_internal_outlinks.csv",
    ]
    expr = (
        "CASE WHEN (SELECT COUNT(*) FROM APP.URLS total WHERE total.IS_INTERNAL = TRUE "
        "AND LOWER(total.CONTENT_TYPE) LIKE 'text/html%') = 0 THEN NULL ELSE CAST("
        "(COALESCE((SELECT ic.NUM_UNIQUE_HYPER_LINKS FROM APP.INLINK_COUNTS ic "
        "WHERE ic.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY), 0) * 100.0) / "
        "(SELECT COUNT(*) FROM APP.URLS total WHERE total.IS_INTERNAL = TRUE "
        "AND LOWER(total.CONTENT_TYPE) LIKE 'text/html%') AS DECIMAL(18, 3)) END"
    )

    for tab in tabs:
        assert _entry(tab, "% of Total") == {
            "csv_column": "% of Total",
            "db_expression": expr,
            "db_table": "APP.URLS",
        }


def test_structured_data_detail_tabs_mark_core_runtime_columns() -> None:
    tabs = [
        "jsonld_urls_detailed_report.csv",
        "microdata_urls_detailed_report.csv",
        "rdfa_urls_detailed_report.csv",
        "validation_errors_detailed_report.csv",
        "validation_warnings_detailed_report.csv",
    ]
    for tab in tabs:
        for column in ["Subject", "Predicate", "Object", "Errors", "Warnings"]:
            assert _entry(tab, column) == {
                "csv_column": column,
                "db_expression": "NULL",
                "db_table": "APP.URLS",
                "runtime_extract": {
                    "type": "structured_data_detailed",
                    "field": column,
                },
            }
        for index in range(1, 11):
            for column in (
                f"Validation Type {index}",
                f"Severity {index}",
                f"Issue {index}",
            ):
                assert _entry(tab, column) == {
                    "csv_column": column,
                    "db_expression": "NULL",
                    "db_table": "APP.URLS",
                    "runtime_extract": {
                        "type": "structured_data_detailed",
                        "field": column,
                    },
                }


def test_pending_link_reports_map_unlinked_flags() -> None:
    expected = {
        "canonicals_nonindexable_canonicals.csv": "APP.MULTIMAP_CANONICALS_PENDING_LINK",
        "pagination_non200_pagination_urls.csv": "APP.MULTIMAP_PAGINATION_PENDING_LINK",
        "pagination_unlinked_pagination_urls.csv": "APP.MULTIMAP_PAGINATION_PENDING_LINK",
    }

    for tab, join_table in expected.items():
        assert _entry(tab, "Unlinked") == {
            "csv_column": "Unlinked",
            "db_expression": (
                "CASE WHEN EXISTS (SELECT 1 FROM "
                f"{join_table} j "
                "WHERE j.MULTIMAP_KEY = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY) "
                "THEN 'true' ELSE 'false' END"
            ),
            "db_table": "APP.URLS",
        }


def test_all_inlinks_and_hreflang_url_tabs_map_unlinked_flags_from_inlink_counts() -> None:
    expected_expr = (
        "CASE WHEN COALESCE((SELECT ic.NUM_HYPER_LINKS FROM APP.INLINK_COUNTS ic "
        "WHERE ic.ENCODED_URL = (SELECT d.ENCODED_URL FROM APP.UNIQUE_URLS d "
        "WHERE d.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY) FETCH FIRST 1 ROWS ONLY), 0) = 0 "
        "THEN 'true' ELSE 'false' END"
    )
    for tab in [
        "all_inlinks.csv",
        "hreflang_non200_hreflang_urls.csv",
        "hreflang_unlinked_hreflang_urls.csv",
    ]:
        assert _entry(tab, "Unlinked") == {
            "csv_column": "Unlinked",
            "db_expression": expected_expr,
            "db_table": "APP.LINKS",
        }


def test_orphan_pages_map_url_to_destination() -> None:
    assert _entry("orphan_pages.csv", "URL") == {
        "csv_column": "URL",
        "db_expression": (
            "(SELECT d.ENCODED_URL FROM APP.UNIQUE_URLS d "
            "WHERE d.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.LINKS",
    }


def test_custom_filter_rollout_maps_filter_counts_across_url_tabs() -> None:
    tabs = [
        "custom_search_all.csv",
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

    for tab in tabs:
        assert _entry(tab, "Filter 1") == {
            "csv_column": "Filter 1",
            "db_expression": (
                "(SELECT cfm.NUM_MATCHES FROM APP.CUSTOM_FILTER_MATCHES cfm "
                "WHERE cfm.ENCODED_URL = APP.URLS.ENCODED_URL AND cfm.FILTER_IDX = 0 "
                "FETCH FIRST 1 ROWS ONLY)"
            ),
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Filter 100") == {
            "csv_column": "Filter 100",
            "db_expression": (
                "(SELECT cfm.NUM_MATCHES FROM APP.CUSTOM_FILTER_MATCHES cfm "
                "WHERE cfm.ENCODED_URL = APP.URLS.ENCODED_URL AND cfm.FILTER_IDX = 99 "
                "FETCH FIRST 1 ROWS ONLY)"
            ),
            "db_table": "APP.URLS",
        }


def test_custom_extractor_first_match_rollout_maps_primary_match_columns() -> None:
    url_tabs = [
        "custom_extraction_all.csv",
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

    for tab in url_tabs:
        assert _entry(tab, "Extractor 1 1") == {
            "csv_column": "Extractor 1 1",
            "db_expression": (
                "(SELECT CAST(ce.MATCHED AS LONG VARCHAR) FROM APP.CUSTOM_EXTRACTION ce "
                "WHERE ce.ENCODED_URL = APP.URLS.ENCODED_URL AND ce.EXTRACTOR_IDX = 0 "
                "FETCH FIRST 1 ROWS ONLY)"
            ),
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Extractor 100 1") == {
            "csv_column": "Extractor 100 1",
            "db_expression": (
                "(SELECT CAST(ce.MATCHED AS LONG VARCHAR) FROM APP.CUSTOM_EXTRACTION ce "
                "WHERE ce.ENCODED_URL = APP.URLS.ENCODED_URL AND ce.EXTRACTOR_IDX = 99 "
                "FETCH FIRST 1 ROWS ONLY)"
            ),
            "db_table": "APP.URLS",
        }

    assert _entry("custom_javascript_all.csv", "Extractor 1 1") == {
        "csv_column": "Extractor 1 1",
        "db_expression": (
            "(SELECT CAST(cj.MATCHED AS LONG VARCHAR) FROM APP.CUSTOM_JAVASCRIPT cj "
            "WHERE cj.ENCODED_URL = APP.URLS.ENCODED_URL AND cj.EXTRACTOR_IDX = 0 "
            "FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.URLS",
    }
    assert _entry("custom_javascript_all.csv", "Extractor 100 1") == {
        "csv_column": "Extractor 100 1",
        "db_expression": (
            "(SELECT CAST(cj.MATCHED AS LONG VARCHAR) FROM APP.CUSTOM_JAVASCRIPT cj "
            "WHERE cj.ENCODED_URL = APP.URLS.ENCODED_URL AND cj.EXTRACTOR_IDX = 99 "
            "FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.URLS",
    }
    assert _entry("all_inlinks.csv", "Extractor 1 1") == {
        "csv_column": "Extractor 1 1",
        "db_expression": (
            "(SELECT CAST(ce.MATCHED AS LONG VARCHAR) FROM APP.CUSTOM_EXTRACTION ce "
            "WHERE ce.ENCODED_URL = (SELECT d.ENCODED_URL FROM APP.UNIQUE_URLS d "
            "WHERE d.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY) AND ce.EXTRACTOR_IDX = 0 "
            "FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.LINKS",
    }
    assert _entry("all_inlinks.csv", "Extractor 100 1") == {
        "csv_column": "Extractor 100 1",
        "db_expression": (
            "(SELECT CAST(ce.MATCHED AS LONG VARCHAR) FROM APP.CUSTOM_EXTRACTION ce "
            "WHERE ce.ENCODED_URL = (SELECT d.ENCODED_URL FROM APP.UNIQUE_URLS d "
            "WHERE d.ID = APP.LINKS.DST_ID FETCH FIRST 1 ROWS ONLY) AND ce.EXTRACTOR_IDX = 99 "
            "FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.LINKS",
    }


def test_custom_extractor_multi_match_rollout_maps_remaining_match_columns() -> None:
    assert _entry("custom_extraction_all.csv", "Extractor 1 2") == {
        "csv_column": "Extractor 1 2",
        "db_column": "ENCODED_URL",
        "db_table": "APP.URLS",
        "multi_row_extract": {
            "type": "custom_extraction_match",
            "source": "encoded_url",
            "extractor_idx": 0,
            "match_index": 2,
            "columns": ["ENCODED_URL"],
        },
    }
    assert _entry("internal_all.csv", "Extractor 100 10") == {
        "csv_column": "Extractor 100 10",
        "db_column": "ENCODED_URL",
        "db_table": "APP.URLS",
        "multi_row_extract": {
            "type": "custom_extraction_match",
            "source": "encoded_url",
            "extractor_idx": 99,
            "match_index": 10,
            "columns": ["ENCODED_URL"],
        },
    }
    assert _entry("internal_html.csv", "Extractor 1 7") == {
        "csv_column": "Extractor 1 7",
        "db_column": "ENCODED_URL",
        "db_table": "APP.URLS",
        "multi_row_extract": {
            "type": "custom_extraction_match",
            "source": "encoded_url",
            "extractor_idx": 0,
            "match_index": 7,
            "columns": ["ENCODED_URL"],
        },
    }
    assert _entry("all_inlinks.csv", "Extractor 1 10") == {
        "csv_column": "Extractor 1 10",
        "db_column": "DST_ID",
        "db_table": "APP.LINKS",
        "multi_row_extract": {
            "type": "custom_extraction_match",
            "source": "dst_id",
            "extractor_idx": 0,
            "match_index": 10,
            "columns": ["DST_ID"],
        },
    }


def test_minimize_main_thread_work_report_maps_pagespeed_blob_breakdown() -> None:
    expected = {
        "csv_column": "Script Evaluation",
        "db_column": "JSON_RESPONSE",
        "db_table": "APP.PAGE_SPEED_API",
        "blob_extract": {
            "type": "pagespeed_main_thread_work",
            "key": "scriptEvaluation",
        },
    }
    assert _entry("minimize_main_thread_work_report.csv", "Script Evaluation") == expected
    assert _entry("minimize_main_thread_work_report.csv", "Rendering") == {
        "csv_column": "Rendering",
        "db_column": "JSON_RESPONSE",
        "db_table": "APP.PAGE_SPEED_API",
        "blob_extract": {
            "type": "pagespeed_main_thread_work",
            "key": "paintCompositeRender",
        },
    }


def test_change_detection_current_state_rollout_maps_exact_current_fields() -> None:
    assert _entry("change_detection_indexability.csv", "Current Indexability") == _entry(
        "internal_all.csv",
        "Indexability",
    ) | {"csv_column": "Current Indexability"}
    assert _entry("change_detection_all.csv", "Current Page Title") == {
        "csv_column": "Current Page Title",
        "db_column": "TITLE_1",
        "db_table": "APP.URLS",
    }
    assert _entry("change_detection_all.csv", "Current Inlinks") == {
        "csv_column": "Current Inlinks",
        "db_expression": (
            "(SELECT ic.NUM_HYPER_LINKS FROM APP.INLINK_COUNTS ic "
            "WHERE ic.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
        ),
        "db_table": "APP.URLS",
    }
    assert _entry("change_detection_word_count.csv", "Current Word Count") == {
        "csv_column": "Current Word Count",
        "db_column": "WORD_COUNT",
        "db_table": "APP.URLS",
    }
    assert _entry("change_detection_word_count.csv", "Crawl Timestamp") == {
        "csv_column": "Crawl Timestamp",
        "db_column": "TIMESTAMP",
        "db_table": "APP.URLS",
    }
    assert _entry(
        "change_detection_unique_external_outlinks.csv",
        "Current Unique External Outlinks",
    ) == {
        "csv_column": "Current Unique External Outlinks",
        "db_column": "NUM_UNIQUE_EXTERNAL_OUTLINKS",
        "db_table": "APP.URLS",
    }


def test_internal_tabs_roll_out_pagespeed_semantic_and_text_ratio_mappings() -> None:
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
    pagespeed_columns = {
        "First Contentful Paint Time (ms)": "FIRST_CONTENTFUL_PAINT",
        "Speed Index Time (ms)": "SPEED_INDEX",
        "Largest Contentful Paint Time (ms)": "LARGEST_CONTENTFUL_PAINT",
        "Time to Interactive (ms)": "TIME_TO_INTERACTIVE",
        "Total Blocking Time (ms)": "TOTAL_BLOCKING_TIME",
        "Total Size Savings (Bytes)": "TOTAL_OPPORTUNITY_SAVINGS_BYTES",
        "Total Time Savings (ms)": "TOTAL_OPPORTUNITY_SAVINGS_MS",
        "Total Page Size (Bytes)": "TOTAL_PAGE_SIZE",
        "HTML Size (Bytes)": "HTML_SIZE",
        "Image Size (Bytes)": "IMAGE_SIZE",
        "CSS Size (Bytes)": "CSS_SIZE",
        "JavaScript Size (Bytes)": "JAVASCRIPT_SIZE",
        "Font Size (Bytes)": "FONT_SIZE",
        "Media Size (Bytes)": "MEDIA_SIZE",
        "Other Size (Bytes)": "OTHER_SIZE",
        "Third Party Size (Bytes)": "THIRD_PARTY_SIZE",
        "Core Web Vitals Assessment": "LOADING_EXPERIENCE_SCORE",
        "CrUX Largest Contentful Paint Time (ms)": "LOADING_EXPERIENCE_LARGEST_CONTENTFUL_PAINT_TIME",
        "CrUX Interaction to Next Paint (ms)": "LOADING_EXPERIENCE_INTERACTION_TO_NEXT_PAINT_MS",
        "CrUX Cumulative Layout Shift": "LOADING_EXPERIENCE_CUMULATIVE_LAYOUT_SHIFT",
        "CrUX First Contentful Paint Time (ms)": "LOADING_EXPERIENCE_FIRST_CONTENTFUL_PAINT_TIME",
        "Eliminate Render-Blocking Resources Savings (ms)": "ELIMINATE_RENDER_BLOCKING_RESOURCES",
        "Defer Offscreen Images Savings (ms)": "DEFER_OFFSCREEN_IMAGES_MS",
        "Efficiently Encode Images Savings (ms)": "EFFICIENTLY_ENCODE_IMAGES_MS",
        "Properly Size Images Savings (ms)": "PROPERLY_SIZE_IMAGES_MS",
        "Minify CSS Savings (ms)": "MINIFY_CSS_MS",
        "Minify JavaScript Savings (ms)": "MINIFY_JAVASCRIPT_MS",
        "Reduce Unused CSS Savings (ms)": "REMOVE_UNUSED_CSS_MS",
        "Reduce Unused JavaScript Savings (ms)": "REMOVE_UNUSED_JAVASCRIPT_MS",
        "Serve Images in Next-Gen Formats Savings (ms)": "NEXT_GEN_IMAGES_MS",
        "Enable Text Compression Savings (ms)": "TEXT_COMPRESSION_MS",
        "Preconnect to Required Origins Savings (ms)": "PRECONNECT_MS",
        "Server Response Times (TTFB) (ms)": "SERVER_RESPONSE_TIMES_SAVINGS_MS",
        "Multiple Redirects Savings (ms)": "REDIRECTS",
        "Preload Key Requests Savings (ms)": "PRELOAD",
        "Use Video Format for Animated Images Savings (ms)": "VIDEO_FORMAT_MS",
        "Total Image Optimization Savings (ms)": "TOTAL_IMAGE_OPTIMIZATION_SAVINGS",
        "Avoid Serving Legacy JavaScript to Modern Browsers Savings (ms)": "LEGACY_JAVASCRIPT",
    }
    semantic_exprs = {
        "Closest Semantically Similar Address": (
            "(SELECT cs.CLOSEST_URL FROM APP.COSINE_SIMILARITY cs "
            "WHERE cs.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
        ),
        "Semantic Similarity Score": (
            "(SELECT cs.SCORE FROM APP.COSINE_SIMILARITY cs "
            "WHERE cs.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
        ),
        "No. Semantically Similar": (
            "(SELECT cs.SIMILAR_URLS FROM APP.COSINE_SIMILARITY cs "
            "WHERE cs.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
        ),
        "Semantic Relevance Score": (
            "(SELECT lr.SCORE FROM APP.LOW_RELEVANCE lr "
            "WHERE lr.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
        ),
    }

    for tab in internal_tabs:
        assert _entry(tab, "Text Ratio") == {
            "csv_column": "Text Ratio",
            "db_column": "TEXT_TO_HTML_RATIO",
            "db_table": "APP.URLS",
        }
        for csv_column, db_column in pagespeed_columns.items():
            assert _entry(tab, csv_column) == {
                "csv_column": csv_column,
                "db_expression": (
                    f"(SELECT psi.{db_column} FROM APP.PAGE_SPEED_API psi "
                    "WHERE psi.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
                ),
                "db_table": "APP.URLS",
            }
        for csv_column, expr in semantic_exprs.items():
            assert _entry(tab, csv_column) == {
                "csv_column": csv_column,
                "db_expression": expr,
                "db_table": "APP.URLS",
            }


def test_link_score_and_transfer_metrics_roll_out_across_url_tabs() -> None:
    link_score_tabs = [
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
        "links_all.csv",
        "links_follow_nofollow_internal_inlinks_to_page.csv",
        "links_internal_nofollow_inlinks_only.csv",
        "links_internal_nofollow_outlinks.csv",
        "links_internal_outlinks_with_no_anchor_text.csv",
        "links_nondescriptive_anchor_text_in_internal_outlinks.csv",
        "links_nonindexable_page_inlinks_only.csv",
        "links_outlinks_to_localhost.csv",
        "links_pages_with_high_crawl_depth.csv",
        "links_pages_with_high_external_outlinks.csv",
        "links_pages_with_high_internal_outlinks.csv",
        "links_pages_without_internal_outlinks.csv",
    ]
    link_score_expr = (
        "(SELECT ls.LINKSCORE FROM APP.LINK_SCORE ls "
        "WHERE ls.ENCODED_URL = APP.URLS.ENCODED_URL FETCH FIRST 1 ROWS ONLY)"
    )
    transfer_tabs = [
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
        "validation_all.csv",
        "validation_body_element_preceding_html.csv",
        "validation_head_not_first_in_html_element.csv",
        "validation_high_carbon_rating.csv",
        "validation_html_document_over_15mb.csv",
        "validation_invalid_html_elements_in_head.csv",
        "validation_missing_body_tag.csv",
        "validation_missing_head_tag.csv",
        "validation_multiple_body_tags.csv",
        "validation_multiple_head_tags.csv",
        "validation_resource_over_15mb.csv",
    ]

    for tab in link_score_tabs:
        assert _entry(tab, "Link Score") == {
            "csv_column": "Link Score",
            "db_expression": link_score_expr,
            "db_table": "APP.URLS",
        }

    for tab in transfer_tabs:
        assert _entry(tab, "Transferred (bytes)") == {
            "csv_column": "Transferred (bytes)",
            "db_column": "PAGE_TRANSFER_SIZE",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "Total Transferred (bytes)") == {
            "csv_column": "Total Transferred (bytes)",
            "db_column": "TOTAL_PAGE_TRANSFER_SIZE",
            "db_table": "APP.URLS",
        }
        assert _entry(tab, "CO2 (mg)") == {
            "csv_column": "CO2 (mg)",
            "db_column": "CO2",
            "db_table": "APP.URLS",
        }
