### Pipeline for WFP Food Prices's Datasets
[![Build Status](https://github.com/OCHA-DAP/hdx-scraper-wfp-foodprices/actions/workflows/run-python-tests.yaml/badge.svg)](https://github.com/OCHA-DAP/hdx-scraper-wfp-foodprices/actions/workflows/run-python-tests.yaml)
[![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-scraper-wfp-foodprices/badge.svg?branch=main&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-scraper-wfp-foodprices?branch=main)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)


This pipeline connects to the [WFP](http://dataviz.vam.wfp.org/) website via
provided [API](https://api.wfp.org/) and extracts food prices data country by
country creating a dataset per country in HDX. It makes in the order of 2000
reads from WFP and 400 read/writes (API calls) to HDX in a one hour period. It
saves 2 temporary files per country each less than 2 MB and these are what are
uploaded to HDX. In addition a 100 MB file is generated and uploaded to HDX.
These files are then deleted. Market and price data are fetched from the WFP API;
each price record is normalised to USD using historical currency exchange rates;
duplicate entries (same flag/date/admin/market/commodity/unit/price type) are
consolidated; and the results are first written to per-country standard datasets (prices and
markets files); a 100 MB global standard food prices dataset is then generated
and uploaded; and finally a HAPI food prices dataset is produced from the global
data. It runs every month.

### Usage

    python run.py

For the script to run, you will need to have a file called .hdx_configuration.yaml in your home directory containing your HDX key eg.

    hdx_key: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
    hdx_read_only: false
    hdx_site: prod

 You will also need to supply the universal .useragents.yaml file in your home directory as specified in the parameter *user_agent_config_yaml* passed to facade in run.py. The collector reads the key **hdx-scraper-wfp-foodprices** as specified in the parameter *user_agent_lookup*.

 Alternatively, you can set up environment variables: USER_AGENT, HDX_KEY, HDX_SITE, TEMP_DIR, LOG_FILE_ONLY
