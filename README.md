### Template Usage

Replace scrapername everywhere with your scraper's name eg. worldbank
Replace ScraperName everywhere with your scraper's name eg. World Bank
Look for xxx and ... and replace add text accordingly.

Scrapers can be installed on QuickCode and set up to run on a schedule 
using the command in the file "crontab".

Collector designed to collect ScraperName datasets from the [ScraperName](http://) website.

For full scrapers following this template see:
[ACLED](https://github.com/OCHA-DAP/hdxscraper-acled-africa),
[FTS](https://github.com/OCHA-DAP/hdxscraper-fts),
[WHO](https://github.com/OCHA-DAP/hdxscraper-who),
[World Bank](https://github.com/OCHA-DAP/hdxscraper-worldbank),
[WorldPop](https://github.com/OCHA-DAP/hdxscraper-worldpop)

For a scraper that also creates datasets disaggregated by indicator (not just country) and
reads metadata from a Google spreadsheet exported as csv, see:
[IDMC](https://github.com/OCHA-DAP/hdxscraper-idmc)

### Collector for ScraperName's Datasets
[![Build Status](https://travis-ci.org/OCHA-DAP/hdxscraper-scrapername.svg?branch=master&ts=1)](https://travis-ci.org/OCHA-DAP/hdxscraper-scrapername) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdxscraper-scrapername/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdxscraper-scrapername?branch=master)

### Usage
python run.py

You will need to have a file called .hdxkey in your home directory containing only your HDX key for the script to run. The script was created to automatically register datasets on the [Humanitarian Data Exchange](http://data.humdata.org/) project.