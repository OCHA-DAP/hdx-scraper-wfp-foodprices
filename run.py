#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download

from wfpfood import generate_dataset_and_showcase, get_countriesdata

# Remove 2 lines below if you don't want emails when there are errors
from hdx.facades import logging_kwargs
logging_kwargs['smtp_config_yaml'] = join('config', 'smtp_configuration.yml')

from hdx.facades.simple import facade
# Use facade below instead of one above if installing scraper on ScraperWiki
#from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)

def main():
    """Generate dataset and create it in HDX"""

    config                 = Configuration.read()

    countries_url          = config['countries_url']
    wfpfood_url            = config['wfpfood_url']
    country_correspondence = config['country_correspondence']

    with Download() as downloader:
        countriesdata = get_countriesdata(countries_url, downloader, country_correspondence)
        logger.info('Number of datasets to upload: %d' % len(countriesdata))
        for countrydata in countriesdata:
            dataset, showcase = generate_dataset_and_showcase(wfpfood_url, downloader, countrydata)
            if dataset:
                dataset.update_from_yaml()
                dataset.create_in_hdx()
                showcase.create_in_hdx()
                showcase.add_dataset(dataset)

if __name__ == '__main__':
#    facade(main, hdx_site='test', user_agent_config_yaml = join(expanduser('~'), '.wfpfooduseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))
    facade(main, hdx_site='test', user_agent_config_yaml = join(expanduser('~'), '.wfpfooduseragent.yml'), project_config_yaml=join('config', 'project_configuration.yml'))
    ## CHANGE THE BOT ID to a proper ID !!!!!


