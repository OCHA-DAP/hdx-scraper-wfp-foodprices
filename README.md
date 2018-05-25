### WFP Food Prices Scraper

Scrapers can be installed on QuickCode and set up to run on a schedule
using the command in the file "crontab".

Collector designed to collect WFP Food Prices datasets from the [WFP](http://dataviz.vam.wfp.org/) website
via provided [API](http://dataviz.vam.wfp.org/api/getfoodprices?ac=1)

### Collector for WFP Food Prices's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdxscraper-wfp-food-prices.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdxscraper-wfp-food-prices) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdxscraper-wfp-food-prices/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdxscraper-wfp-food-prices?branch=master)

### Usage
python run.py

You will need to have a file called .hdxkey in your home directory containing only your HDX key for the script to run. The script was created to automatically register datasets on the [Humanitarian Data Exchange](http://data.humdata.org/) project.