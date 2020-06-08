#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join, expanduser

from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir, progress_storing_tempdir

from wfpfood import generate_dataset_and_showcase, get_countries, generate_joint_dataset_and_showcase

from hdx.facades.simple import facade

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-wfp-foodprices'


def main():
    """Generate dataset and create it in HDX"""

    with temp_dir('wfp-foodprices') as folder:
        with Download() as downloader:
            configuration = Configuration.read()

            countries_path = join('config', configuration['countries_filename'])
            wfpfood_url = configuration['wfpfood_url']
            country_correspondence = configuration['country_correspondence']
            shortcuts = configuration['shortcuts']

            countries = get_countries(countries_path, downloader, country_correspondence)
            logger.info('Number of datasets to upload: %d' % len(countries))

            batch = None
            for info, country in progress_storing_tempdir('WFPFoodPrices', countries, 'iso3'):
                dataset, showcase = generate_dataset_and_showcase(wfpfood_url, downloader, info['folder'], country, shortcuts)
                batch = info['batch']
                if dataset:
                    dataset.update_from_yaml()
                    dataset['notes'] = dataset['notes'] % 'Food Prices data for %s. Food prices data comes from the World Food Programme and covers' % country['name']
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: WFP Food Prices', batch=batch)
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)
                    dataset.generate_resource_view(1)

            logger.info('Individual country datasets finished.')

            dataset, showcase, file_csv = generate_joint_dataset_and_showcase(wfpfood_url, downloader, folder, countries)
            dataset.update_from_yaml()
            dataset['notes'] = dataset['notes'] % 'Global Food Prices data from the World Food Programme covering'
            dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False, updated_by_script='HDX Scraper: WFP Food Prices', batch=batch)
            showcase.create_in_hdx()
            showcase.add_dataset(dataset)
            dataset.get_resource().create_datastore_from_yaml_schema(yaml_path="wfp_food_prices.yml", path=file_csv,
                                                                     delete_first=1)


    logger.info('Done')


if __name__ == '__main__':
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup, project_config_yaml=join('config', 'project_configuration.yml'))


