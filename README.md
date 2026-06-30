# Pipeline for WFP Food Prices Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-wfp-foodprices/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-wfp-foodprices/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-wfp-foodprices/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-wfp-foodprices?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This pipeline connects to the [WFP](http://dataviz.vam.wfp.org/) website via
provided [API](https://api.wfp.org/) and extracts food prices data country by
country creating a dataset per country in HDX. It makes in the order of 2000
reads from WFP and 400 read/writes (API calls) to HDX. It saves 2 temporary
files per country each less than 2 MB and these are what are uploaded to HDX. In
addition a 100 MB file is generated and uploaded to HDX. These files are then
deleted. Market and price data are fetched from the WFP API; each price record
is normalised to USD using historical currency exchange rates; duplicate entries
(same flag/date/admin/market/commodity/unit/price type) are consolidated; and
the results are first written to per-country standard datasets (prices and
markets files); a 100 MB global standard food prices dataset is then generated
and uploaded; and finally a HAPI food prices dataset is produced from the global
data. It runs every Sunday at around 8 AM UTC and takes approximately 12 hours
to complete.

## Data Pipeline

### API reads (~2000 calls per run)

- **WFP API reads** (~2000 reads): market and price data fetched per country from
  the WFP API.
- **HDX reads** (~400 reads): metadata reads for existing per-country datasets
  before updating.

### API writes (~400 calls per run)

- **Per-country datasets** (~one write per country): each country dataset contains
  2 files — a prices CSV and a markets CSV, each less than 2 MB.
- **Global food prices dataset** (1 write): a single ~100 MB CSV aggregating all
  country data.
- **HAPI food prices dataset** (1 write): derived from the global data.

### Temporary files

- 2 files per country (prices and markets), each less than 2 MB.
- 1 global ~100 MB file, generated and uploaded then deleted.

### Uploaded files

- Per-country datasets: prices CSV and markets CSV per country, each less than
  2 MB.
- Global food prices CSV (~100 MB).
- HAPI food prices dataset.

### Transformations

1. **USD normalisation**: each price record is converted to USD using historical
   currency exchange rates.
2. **Deduplication**: duplicate entries (matching on flag, date, admin area,
   market, commodity, unit, and price type) are consolidated into a single row.

## Development

### Environment

Development is currently done using Python 3.13. The environment can be created with:

```shell
    uv sync
```

This creates a .venv folder with the versions specified in the project's uv.lock file.

### Installing and running

For the script to run, you will need to have a file called
.hdx_configuration.yaml in your home directory containing your HDX key, e.g.:

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home
 directory as specified in the parameter *user_agent_config_yaml* passed to
 facade in run.py. The collector reads the key **hdx-scraper-wfp-foodprices**
 as specified in the parameter *user_agent_lookup*.

 Alternatively, you can set up environment variables: `USER_AGENT`, `HDX_KEY`,
`HDX_SITE`, `TEMP_DIR`, `LOG_FILE_ONLY`.

Country run (per-country datasets):

```shell
    uv run python run.py
```

World/global run (global and HAPI datasets):

```shell
    uv run python run2.py
```

### Pre-commit

pre-commit will be installed when syncing uv. It is run every time you make a git
commit if you call it like this:

```shell
    pre-commit install
```

With pre-commit, all code is formatted according to
[ruff](https://docs.astral.sh/ruff/) guidelines.

To check if your changes pass pre-commit without committing, run:

```shell
    pre-commit run --all-files
```

## Packages

[uv](https://github.com/astral-sh/uv) is used for package management.  If
you've introduced a new package to the source code (i.e. anywhere in `src/`),
please add it to the `project.dependencies` section of `pyproject.toml` with
any known version constraints.

To add packages required only for testing, add them to the
`[dependency-groups]`.

Any changes to the dependencies will be automatically reflected in
`uv.lock` with `pre-commit`, but you can re-generate the files without committing by
executing:

```shell
    uv lock --upgrade
```

## Project

[uv](https://github.com/astral-sh/uv) is used for project management. The project can be
built using:

```shell
    uv build
```

Linting and syntax checking can be run with:

```shell
    uv run ruff check
```

To run the tests and view coverage, execute:

```shell
    uv run pytest
```
