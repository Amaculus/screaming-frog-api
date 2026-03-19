# Data Platform And Warehousing

This file focuses on using crawl data as part of a broader analytics stack.

## Core idea
Treat each crawl as a structured technical snapshot of the site.

## Common warehouse tables
- page-state fact table
- link-edge fact table
- crawl metadata dimension
- issue summary fact table
- crawl-diff event table

## Common joins
- Search Console impressions and clicks
- analytics landing-page sessions
- log-file crawl behavior
- CMS ownership metadata
- product or revenue attributes

## High-value outputs
- prioritized issue scoring models
- template quality dashboards
- technical health trends over time
- discovery and internal-linking trend models
- SEO forecasting inputs

## Why the library fits
- It exposes typed data for common workflows.
- It preserves raw Derby access for unmapped questions.
- It can feed warehouse jobs without requiring the GUI.
