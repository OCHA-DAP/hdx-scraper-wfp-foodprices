#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join
from typing import Dict

from hdx.api.configuration import Configuration
from hdx.database import Database
from hdx.facades.keyword_arguments import facade
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.currency_setup import setup_currency
from hdx.scraper.wfp.foodprices.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.db_updater import DBUpdater
from hdx.scraper.wfp.foodprices.wfp_food import WFPFood
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-wfp-foodprices"


def main(
    save: bool = False,
    use_saved: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
    Returns:
        None
    """
    with Download(
        fail_on_missing_file=False,
        extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
        extra_params_lookup=lookup,
    ) as token_downloader:
        with Download(
            use_env=False, rate_limit={"calls": 1, "period": 0.1}
        ) as downloader:
            with wheretostart_tempdir_batch(lookup) as info:
                folder = info["folder"]
                batch = info["batch"]
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                params = {
                    "dialect": "sqlite",
                    "database": f"/{folder}/foodprices.sqlite",
                }
                with Database(**params) as database:
                    configuration = Configuration.read()
                    wfp_api = WFPAPI(token_downloader, retriever)
                    wfp_api.update_retry_params(attempts=5, wait=3600)
                    session = database.get_session()
                    wfp = WFPMappings(
                        configuration, wfp_api, retriever, session
                    )
                    iso3_to_showcase_url = wfp.read_region_mapping()
                    iso3_to_source = wfp.read_source_overrides()
                    countries = wfp.get_countries()
                    logger.info(
                        f"Number of country datasets to upload: {len(countries)}"
                    )
                    commodity_to_category = (
                        wfp.build_commodity_category_mapping()
                    )
                    setup_currency(retriever, wfp_api)
                    dataset_generator = DatasetGenerator(
                        configuration,
                        folder,
                        iso3_to_showcase_url,
                        iso3_to_source,
                    )
                    dbupdater = DBUpdater(configuration, session)

                    def process_country(country: Dict[str, str]) -> None:
                        countryiso3 = country["iso3"]
                        dataset, showcase = (
                            dataset_generator.get_dataset_and_showcase(
                                countryiso3
                            )
                        )
                        if not dataset:
                            return
                        wfp_food = WFPFood(
                            countryiso3,
                            configuration,
                            iso3_to_showcase_url.get(countryiso3),
                            iso3_to_source.get(countryiso3),
                            commodity_to_category,
                        )
                        if not wfp_food.get_price_markets(wfp_api):
                            return
                        rows, markets, sources = wfp_food.generate_rows()
                        dataset, qc_indicators = (
                            dataset_generator.complete_dataset(
                                countryiso3, dataset, rows, markets, sources
                            )
                        )
                        dbmarkets = wfp_food.get_dbmarkets()
                        time_period = dataset.get_time_period()
                        hdx_url = dataset.get_hdx_url()
                        dbupdater.update_tables(
                            countryiso3, dbmarkets, time_period, hdx_url
                        )

                        snippet = f"Food Prices data for {country['name']}"
                        if dataset:
                            dataset.update_from_yaml()
                            dataset["notes"] = dataset["notes"] % (snippet, "")
                            dataset.generate_quickcharts(
                                -1, indicators=qc_indicators
                            )
                            dataset.create_in_hdx(
                                remove_additional_resources=True,
                                hxl_update=False,
                                updated_by_script="HDX Scraper: WFP Food Prices",
                                batch=batch,
                            )
                            if showcase:
                                showcase.create_in_hdx()
                                showcase.add_dataset(dataset)
                            else:
                                logger.info(
                                    f"{country['name']} does not have a showcase!"
                                )
                        session.commit()

                    for _, country in progress_storing_folder(
                        info, countries, "iso3"
                    ):
                        process_country(country)
                    table_data, start_date, end_date = (
                        dbupdater.get_data_from_tables()
                    )
                    dataset, showcase = (
                        dataset_generator.generate_global_dataset_and_showcase(
                            table_data, start_date, end_date
                        )
                    )
                    snippet = "Countries, Commodities and Markets data"
                    snippet2 = "The volume of data means that the actual Food Prices data is in country level datasets. "
                    dataset.update_from_yaml()
                    dataset["notes"] = dataset["notes"] % (snippet, snippet2)
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script="HDX Scraper: WFP Food Prices",
                        batch=batch,
                    )
                    showcase.create_in_hdx()
                    showcase.add_dataset(dataset)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
