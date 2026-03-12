# Filter Implementation TODO

Tracks unresolved filter stubs in `screamingfrog/filters/`. Each item either
needs implementation, a GitHub issue (SF limitation), or a doc update.

---

## Status Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Done — verified against real crawl data |
| ⚠️ | Implemented — needs verification with suitable crawl data |
| ❌ | Confirmed unimplementable — SF does not persist this data in Derby |
| 📋 | Issue to open against SF (upstream limitation) |

---

## Confirmed Unimplementable — Issue Batch 1

All verified via `mapping.json` and/or real crawl inspection.
Needs one grouped GitHub issue, then descriptions updated.

| Filter file | Filter name | Evidence |
|---|---|---|
| `response_codes.py` | Redirection (JavaScript) | JS redirect hop absent from DB entirely (verified) |
| `response_codes.py` | Internal Redirection (JavaScript) | Same |
| `response_codes.py` | External Redirection (JavaScript) | Same |
| `response_codes.py` | Internal Redirect Loop | No DB column; detected at runtime only |
| `page_titles.py` | pixel-width filters (×2) | `mapping.json`: `db_expression: NULL` |
| `meta_description.py` | pixel-width filters (×2) | `mapping.json`: `db_expression: NULL` |
| `structured_data.py` | Validation Errors | No DB column |
| `structured_data.py` | Validation Warnings | No DB column |

**Action:** Open one grouped GitHub issue. Update each `FilterDef.description`.

---

## Hreflang — Status

### ✅ Not Using Canonical

Implemented and verified via synthetic test crawl. EXISTS on LINK_TYPE=13
where DST has `IS_CANONICALISED = true`. Correctly returned both the page
with a canonicalised hreflang target AND the canonicalised page itself.

### ❌ Unlinked hreflang URLs

Not implementable. SF identifies these at the link level (`APP.LINKS`), not
the URL level. `FilterDef` is based on `APP.URLS`. No MULTIMAP table exists
for this concept. → Issue batch 2.

### ❌ Incorrect Language & Region Codes

Not implementable. No Derby column or table stores the BCP 47 validation
result. SF validates at runtime only. → Issue batch 2.

---

## Issue Batch 2

Open one grouped GitHub issue covering:

- Unlinked hreflang URLs
- Incorrect Language & Region Codes

---

## Limits Doc Update

`features.d/12_limits_and_gaps.md` currently says "All 15 filters" for
Hreflang, "All 10 filters" for Page Titles, "All 12 filters" for Structured
Data. Update to reflect partial/unsupported filters and reasons.

---

## Execution Order

```
1. Open issue batch 1 (confirmed unimplementable)           ~30 min
2. Open issue batch 2 (hreflang unimplementable)            ~15 min
3. Update FilterDef descriptions (remove TODOs)             ~20 min  ← partially done
4. Update 12_limits_and_gaps.md                             ~30 min
5. Verify "Not Using Canonical" with suitable crawl data    when available
```
