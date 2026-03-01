from __future__ import annotations

from screamingfrog.config import ConfigPatches, CustomJavaScript, CustomSearch


def test_config_patches_custom_search_and_js() -> None:
    patches = ConfigPatches()
    patches.set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")
    patches.add_custom_search(
        CustomSearch(name="Filter 1", query=".*", data_type="REGEX", scope="HTML")
    )
    patches.add_custom_javascript(
        CustomJavaScript(name="Extractor 1", javascript="return document.title;")
    )

    payload = patches.to_dict()

    assert payload["mCrawlConfig.mRenderingMode"] == "JAVASCRIPT"
    assert payload["custom_searches"][0]["name"] == "Filter 1"
    assert payload["custom_searches"][0]["dataType"] == "REGEX"
    assert payload["custom_javascript"][0]["type"] == "EXTRACTION"
    assert payload["custom_javascript"][0]["content_types"] == "text/html"
