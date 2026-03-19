# Notebooks And Data Science Workflows

## Exploratory Analysis
- Load crawl artifacts directly into notebooks for ad hoc investigation.
- Move from typed views into raw SQL when a question outpaces current mappings.
- Join crawl outputs with Pandas or Polars pipelines for deeper analysis.
- Persist notebooks against fixed crawl snapshots for reproducibility.

## Feature Engineering
- Build page-level datasets from response, metadata, link, and content signals.
- Derive section, template, or depth-based features from crawl URLs.
- Create candidate scoring models for prioritization or anomaly detection.
- Compare crawl snapshots as labeled before/after datasets.

## Graph And Clustering Work
- Export inlink and outlink relationships into graph libraries.
- Model hub/spoke structures, directory transitions, and weak-link clusters.
- Combine semantic similarity with internal link signals.
- Create cluster review queues for content consolidation work.

## Research And Prototyping
- Test new audit heuristics against stored crawls without rerunning the spider.
- Validate whether a proposed metric is present in Derby, derivable, or blocked.
- Build notebook-backed proofs of concept before hardening them into library features.
- Preserve exploratory analysis as a path toward future stable APIs.
