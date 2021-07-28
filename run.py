#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import argparse
import logging
from os.path import join, expanduser

from hdx.database import Database
from hdx.hdx_configuration import Configuration
from hdx.utilities.downloader import Download
from hdx.utilities.path import get_temp_dir, progress_storing_tempdir

from hdx.facades.keyword_arguments import facade
from hdx.utilities.retriever import Retrieve

from wfpfood import WFPFood

logger = logging.getLogger(__name__)

lookup = 'hdx-scraper-wfp-foodprices'


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-sv', '--save', default=False, action='store_true', help='Save downloaded data')
    parser.add_argument('-usv', '--use_saved', default=False, action='store_true', help='Use saved data')
    args = parser.parse_args()
    return args


def main(save, use_saved, **ignore):
    """Generate dataset and create it in HDX"""

    with Download(extra_params_yaml=join(expanduser('~'), '.extraparams.yml'), extra_params_lookup=lookup) as token_downloader:
        configuration = Configuration.read()
        with Download() as downloader:
            folder = get_temp_dir(lookup)
            retriever = Retrieve(downloader, folder, 'saved_data', folder, save, use_saved)
            params = {'driver': 'sqlite', 'database': f'/{folder}/foodprices.sqlite'}
            with Database(**params) as session:
                wfp = WFPFood(configuration, folder, token_downloader, retriever, session)
                countries = wfp.get_countries()
                logger.info('Number of country datasets to upload: %d' % len(countries))
                wfp.build_mappings()
                for info, country in progress_storing_tempdir(lookup, countries, 'iso3'):
                    countryiso3 = country['iso3']
                    batch = info['batch']
                    dataset, showcase, qc_indicators = wfp.generate_dataset_and_showcase(countryiso3)
                    if dataset:
                        dataset.update_from_yaml()
                        dataset['notes'] = dataset['notes'] % 'Food Prices data for %s. Food prices data comes from the World Food Programme and covers' % country['name']
                        dataset.generate_resource_view(-1, indicators=qc_indicators)
                        dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False,
                                              updated_by_script='HDX Scraper: WFP Food Prices', batch=batch)
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)
                    wfp.update_database()
                dataset, showcase = wfp.generate_global_dataset_and_showcase()
                if dataset:
                    dataset.update_from_yaml()
                    dataset['notes'] = dataset['notes'] % 'Countries, Commodities and Markets data which comes from the World Food Programme. The volume of data means that the actual Food Prices data is in country level datasets. These cover'
                    dataset.create_in_hdx(remove_additional_resources=True, hxl_update=False,
                                          updated_by_script='HDX Scraper: WFP Food Prices', batch=batch)
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == '__main__':
    args = parse_args()
    facade(main, user_agent_config_yaml=join(expanduser('~'), '.useragents.yml'), user_agent_lookup=lookup,
           project_config_yaml=join('config', 'project_configuration.yml'), save=args.save, use_saved=args.use_saved)


