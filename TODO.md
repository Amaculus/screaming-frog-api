# Filter Implementation TODO

Tracks unresolved filter stubs in `screamingfrog/filters/`. Each item either
needs implementation, a GitHub issue (SF limitation), or a doc update.

---

## Status Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Done — verified against real crawl data |
| 🔍 | Needs investigation before a decision can be made |
| ❌ | Confirmed unimplementable — SF does not persist this data in Derby |
| 📋 | Issue to open against SF (upstream limitation) |

---

## Confirmed Unimplementable — Issue Batch 1

These have been verified via `mapping.json` and/or real crawl inspection.
All need a single grouped GitHub issue opened, then the `TODO` descriptions
updated to explain why.

| Filter file | Filter name | Evidence |
|---|---|---|
| `response_codes.py` | Redirection (JavaScript) | Verified: JS redirect hop absent from DB entirely (takko.com test) |
| `response_codes.py` | Internal Redirection (JavaScript) | Same — SF GUI also shows 0 despite confirmed JS redirect in chain |
| `response_codes.py` | External Redirection (JavaScript) | Same |
| `response_codes.py` | Internal Redirect Loop | No DB column; SF detects at runtime, not persisted |
| `page_titles.py` | Over X Pixels (title) | `mapping.json`: `db_expression: NULL` |
| `page_titles.py` | (any pixel-width title filter) | Same |
| `meta_description.py` | Over X Pixels (description) | `mapping.json`: `db_expression: NULL` |
| `meta_description.py` | (any pixel-width meta filter) | Same |
| `structured_data.py` | Validation Errors | No DB column — SF validates at runtime |
| `structured_data.py` | Validation Warnings | Same |

**Action:** Open one grouped GitHub issue. Update each `FilterDef.description`
to replace `TODO: DB columns` with a short factual note, e.g.:
`"Not stored in Derby — SF computes this at runtime only."`

---

## Hreflang — Requires Investigation First

These three stubs in `hreflang.py` need more work before we decide.

### 🔍 Unlinked hreflang URLs

SF's GUI definition: URLs referenced in `hreflang` tags that SF couldn't
crawl / aren't in the crawl at all.

**What we know:**
- The MULTIMAP tables have `APP.LINKS` as their logical base, not `APP.URLS`
- The CSV mapping entry for this filter shows key fields as NULL
- No confirmed MULTIMAP table for "unlinked" has been found yet

**What we don't know:**
- Whether SF stores this in a MULTIMAP table we haven't looked at, or not at all

**Decision path:**
1. Full Derby schema dump — list all table names and columns
2. If a candidate table exists: crawl synthetic site (see below), confirm
3. If nothing found: ❌ → issue batch 2

---

### 🔍 Incorrect Language & Region Codes

SF's GUI definition: `hreflang` attribute values that don't match BCP 47
(e.g., `hreflang="xx-ZZ"`, `hreflang="english"`).

**What we know:**
- No column in `APP.URLS` or `APP.LINKS` stores a validation flag
- `APP.LANGUAGE_ERROR` is for spelling/grammar, not hreflang validation
- `mapping.json` shows no db_expression for this concept

**Decision path:**
1. Schema dump — look for any validation/error flag on hreflang links
2. If nothing: ❌ → issue batch 2
3. If candidate column found: synthetic site test to confirm

---

### 🔍 Not Using Canonical

SF's GUI definition: Pages where the `hreflang` self-reference (or the URL
that carries the hreflang set) is not the canonical version of that URL.
Distinct from "Non-Canonical Return Links" (already implemented via
`MULTIMAP_HREF_LANG_CANONICAL_CONFIRMATION`).

**What we know:**
- "Non-Canonical Return Links" is already implemented and uses the canonical
  confirmation MULTIMAP table
- "Not Using Canonical" is a *different* filter — exact SF semantics unclear
- The mapping CSV for this filter had all-NULL columns in our test crawl

**What we don't know:**
- Whether there is a second canonical-related MULTIMAP table, or whether this
  filter is the same table with a different join direction
- Whether the takko.com data (which had real non-canonical hreflang cases)
  would populate this

**Decision path:**
1. Schema dump — look for second canonical-related MULTIMAP table
2. Re-query `MULTIMAP_HREF_LANG_CANONICAL_CONFIRMATION` with takko crawl,
   check if "Not Using Canonical" maps to a different column/join
3. If nothing: ❌ → issue batch 2

---

## Synthetic Test Site

If the schema dump doesn't clarify the hreflang unknowns, build a minimal
test site served locally with `python -m http.server`.

**What it needs to contain:**

```
/en/          → hreflang: en → /en/, de → /de/, x-default → /en/
/de/          → hreflang: de → /de/, en → /en/
/orphan/      → referenced by hreflang on /en/ but has NO return hreflang tag
/badlang/     → hreflang="xx-ZZ" (invalid BCP 47 code)
/redirect/    → 301 → /en/  (so /de/ with hreflang pointing to /redirect/ = non-canonical)
```

**No redirect complexity for Unlinked** — just a page that IS referenced in a
hreflang annotation but doesn't appear elsewhere in the crawl. Redirect only
needed for "Not Using Canonical" testing.

**SF crawl config for this:**
- JS rendering: OFF (static HTML is enough)
- "Always follow redirects": ON
- Start URL: `http://localhost:8000/en/`
- Include subfolders of localhost

**After crawling:** inspect all MULTIMAP tables + any new tables for data.

---

## Issue Batch 2 (pending Phase 2 outcome)

If the three hreflang unknowns confirm unimplementable, open a second grouped
issue covering:

- Unlinked hreflang URLs
- Incorrect Language & Region Codes
- Not Using Canonical

---

## Limits Doc Update

`features.d/12_limits_and_gaps.md` currently says:

> - **Hreflang**: All 15 filters
> - **Page Titles**: All 10 filters
> - **Structured Data**: All 12 filters

This is inaccurate. After the issues are opened and descriptions updated,
revise this file to list which filters are partial/unsupported and why.

---

## Execution Order

```
1. Schema dump (Derby: list all tables + columns)        ~30 min
2. Check each hreflang unknown against schema            ~30 min
3a. If schema resolves them → implement or issue
3b. If not → build synthetic site, crawl, re-check      ~2–3 h
4. Open issue batch 1 (confirmed unimplementable)        ~30 min
5. Open issue batch 2 (if hreflang also unimplementable) ~15 min
6. Update FilterDef descriptions (remove TODOs)          ~30 min
7. Update 12_limits_and_gaps.md                          ~30 min
```

Steps 1–3 have a dependency chain. Steps 4–7 are independent of each other
and can be done in any order after step 3.
