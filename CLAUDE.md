# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**hdx-scraper-wfp-foodprices** connects to the [WFP](http://dataviz.vam.wfp.org/) website via its [API](https://api.wfp.org/) and extracts food prices data. It creates a dataset per country in HDX (country run), and also produces a global food prices dataset and a HAPI food prices dataset (world run). It runs monthly.

## Commands

Install dependencies:
```bash
uv sync
```

Run the country scraper:
```bash
uv run python run.py
```

Run the world/global scraper:
```bash
uv run python run2.py
```

Run tests:
```bash
uv run pytest
```

Run a single test:
```bash
uv run pytest tests/test_country.py
```

Lint check:
```bash
pre-commit run --all-files
```

## Architecture

Two separate entry points:

- **`run.py`** — invokes `hdx.scraper.wfp.foodprices.country` — fetches food prices per country from the WFP API, normalises prices to USD using historical exchange rates, deduplicates records, and uploads per-country datasets to HDX.
- **`run2.py`** — invokes `hdx.scraper.wfp.foodprices.world` — generates and uploads the 100 MB global standard food prices dataset and the HAPI food prices dataset.

### Key design points

- **Two-phase pipeline**: country run first (produces per-country data), world run second (aggregates into global and HAPI datasets).
- **Config files**: `src/hdx/scraper/wfp/foodprices/config/` holds shared config; each subpackage (`country/`, `world/`) may have its own config subdirectory.

## Environment

Requires `~/.hdx_configuration.yaml` with HDX credentials, or env vars: `HDX_KEY`, `HDX_SITE`, `USER_AGENT`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Requires `~/.useragents.yaml` with a `hdx-scraper-wfp-foodprices` entry.

## Collaboration Style

- Be objective, not agreeable. Act as a partner, not a sycophant. Push back when you disagree, flag tradeoffs honestly, and don't sugarcoat problems.
- Keep explanations brief and to the point.
- Don't rely on recalled knowledge for facts that could be stale (API behaviour, library versions, external systems). Search or read the actual source first.

## Scope of Changes

When fixing a bug or addressing PR feedback, change only what is necessary to resolve the specific issue. Do not refactor surrounding code, rename variables, adjust formatting, or make improvements in the same commit unless they are directly required by the fix.
