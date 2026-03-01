"""Post 9: Programmatic config building"""
# carbon.sh caption: "Build Screaming Frog configs in Python. Ship them to your team."

from screamingfrog import ConfigPatches, CustomSearch, CustomJavaScript

patches = ConfigPatches()

patches.set("mCrawlConfig.mRenderingMode", "JAVASCRIPT")

patches.add_custom_search(
    CustomSearch(name="Product Pages", query="/products/.*", data_type="REGEX")
)

patches.add_custom_javascript(
    CustomJavaScript(
        name="Extract Price",
        javascript="return document.querySelector('.price')?.textContent;"
    )
)

print(patches.to_json()[:500])
