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
