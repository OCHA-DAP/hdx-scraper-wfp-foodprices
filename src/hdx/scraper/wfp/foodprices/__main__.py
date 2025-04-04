#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import gc
import logging
from os.path import expanduser, join
from typing import Dict

import hdx.location.int_timestamp
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.user import User
from hdx.database import Database
from hdx.facades.infer_arguments import facade
from hdx.location.currency import Currency
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices._version import __version__
from hdx.scraper.wfp.foodprices.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.db_updater import DBUpdater
from hdx.scraper.wfp.foodprices.global_prices import get_global_prices_rows
from hdx.scraper.wfp.foodprices.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.wfp.foodprices.hapi_output import HAPIOutput
from hdx.scraper.wfp.foodprices.utilities import get_now, setup_currency
from hdx.scraper.wfp.foodprices.wfp_food import WFPFood
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-wfp-foodprices"
updated_by_script = "HDX Scraper: WFP Food Prices"


def main(
    save: bool = False,
    countryiso3s: str = "",
    use_saved: bool = False,
    save_wfp_rates: bool = True,
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save all downloaded data. Defaults to False.
        countryiso3s (str): Whether to limit to specific countries. Defaults to not limiting ("").
        use_saved (bool): Use saved data. Defaults to False.
        save_wfp_rates (bool): Save WFP FX rates data. Defaults to True.
        err_to_hdx (bool): Whether to write any errors to HDX metadata. Defaults to False.

    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    if not User.check_current_user_organization_access(
        "3ecac442-7fed-448d-8f78-b385ef6f84e7", "create_dataset"
    ):
        raise PermissionError("API Token does not give access to WFP organisation!")
    with HDXErrorHandler(write_to_hdx=err_to_hdx) as error_handler:
        with Download(
            fail_on_missing_file=False,
            extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
            extra_params_lookup=lookup,
        ) as token_downloader:
            with Download(
                use_env=False, rate_limit={"calls": 1, "period": 0.1}
            ) as downloader:
                with wheretostart_tempdir_batch(lookup) as info:
                    if countryiso3s:
                        countryiso3s = countryiso3s.split(",")
                    else:
                        countryiso3s = None
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
                        now = get_now(retriever)
                        wfp_api = WFPAPI(token_downloader, retriever)
                        wfp_api.update_retry_params(attempts=5, wait=3600)
                        wfp = WFPMappings(configuration, wfp_api, retriever)
                        iso3_to_showcase_url = wfp.read_region_mapping()
                        iso3_to_source = wfp.read_source_overrides()
                        countries = wfp.get_countries(countryiso3s)
                        logger.info(
                            f"Number of country datasets to upload: {len(countries)}"
                        )
                        commodity_to_category, dbcommodities = (
                            wfp.build_commodity_category_mapping()
                        )
                        if save_wfp_rates:
                            wfp_rates_folder = folder
                        else:
                            wfp_rates_folder = None
                        currencies = setup_currency(
                            now, retriever, wfp_api, wfp_rates_folder
                        )
                        dataset_generator = DatasetGenerator(
                            configuration,
                            folder,
                            iso3_to_showcase_url,
                            iso3_to_source,
                            currencies,
                        )
                        dbupdater = DBUpdater(configuration, database)
                        dbupdater.update_commodities(dbcommodities)

                        def process_country(country: Dict[str, str]) -> None:
                            countryiso3 = country["iso3"]
                            dataset, showcase = (
                                dataset_generator.get_dataset_and_showcase(countryiso3)
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
                            dbmarkets = wfp_food.get_price_markets(wfp_api)
                            if not dbmarkets:
                                return
                            rows, markets, sources = wfp_food.generate_rows(dbmarkets)
                            dataset, qc_indicators = dataset_generator.complete_dataset(
                                countryiso3, dataset, rows, markets, sources
                            )
                            time_period = dataset.get_time_period()
                            hdx_url = dataset.get_hdx_url()
                            dbupdater.update_tables(
                                countryiso3,
                                time_period,
                                hdx_url,
                                dbmarkets,
                            )

                            snippet = f"Food Prices data for {country['name']}"
                            if dataset:
                                dataset.update_from_yaml(
                                    script_dir_plus_file(
                                        join("config", "hdx_dataset_static.yaml"),
                                        main,
                                    )
                                )
                                dataset["notes"] = dataset["notes"] % (snippet, "")
                                dataset.generate_quickcharts(
                                    -1, indicators=qc_indicators
                                )
                                dataset.create_in_hdx(
                                    remove_additional_resources=True,
                                    hxl_update=False,
                                    updated_by_script=updated_by_script,
                                    batch=batch,
                                )
                                if showcase:
                                    showcase.create_in_hdx()
                                    showcase.add_dataset(dataset)
                                else:
                                    logger.info(
                                        f"{country['name']} does not have a showcase!"
                                    )

                        for _, country in progress_storing_folder(
                            info, countries, "iso3"
                        ):
                            process_country(country)

                        del hdx.location.int_timestamp._cache_timestamp_lookup
                        del Currency._cached_current_rates
                        del Currency._cached_historic_rates
                        del Currency._secondary_rates
                        del Currency._secondary_historic_rates
                        gc.collect()

                        table_data, start_date, end_date = (
                            dbupdater.get_data_from_tables()
                        )
                        global_prices_info = get_global_prices_rows(downloader, folder)
                        dataset, showcase = (
                            dataset_generator.generate_global_dataset_and_showcase(
                                global_prices_info, table_data, start_date, end_date
                            )
                        )
                        snippet = "Countries, Commodities and Markets data"
                        snippet2 = "The volume of data means that the actual Food Prices data is in country level datasets. "
                        dataset.update_from_yaml(
                            script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"), main
                            )
                        )
                        dataset["notes"] = dataset["notes"] % (snippet, snippet2)
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            hxl_update=False,
                            updated_by_script=updated_by_script,
                            batch=batch,
                        )
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)

                        prices_resource_id = None
                        markets_resource_id = None
                        for resource in dataset.get_resources():
                            resource_name = resource["name"]
                            if resource_name == dataset_generator.global_prices_name:
                                prices_resource_id = resource["id"]
                            elif resource_name == dataset_generator.global_markets_name:
                                markets_resource_id = resource["id"]
                            elif (
                                resource_name
                                == dataset_generator.global_commodities_name
                            ):
                                commodities_resource_id = resource["id"]
                            elif (
                                resource_name
                                == dataset_generator.global_currencies_name
                            ):
                                currencies_resource_id = resource["id"]
                        if prices_resource_id and markets_resource_id:
                            dataset_id = dataset["id"]
                            hapi_output = HAPIOutput(
                                configuration,
                                error_handler,
                            )
                            hapi_output.setup_admins(retriever, countryiso3s)
                            hapi_currencies = hapi_output.process_currencies(
                                currencies, dataset_id, currencies_resource_id
                            )
                            hapi_commodities = hapi_output.process_commodities(
                                table_data["DBCommodity"],
                                dataset_id,
                                commodities_resource_id,
                            )
                            hapi_markets = hapi_output.process_markets(
                                table_data["DBMarket"], dataset_id, markets_resource_id
                            )
                            hapi_prices = hapi_output.process_prices(
                                global_prices_info, dataset_id, prices_resource_id
                            )
                            gc.collect()
                            hapi_dataset_generator = HAPIDatasetGenerator(
                                configuration,
                                folder,
                                global_prices_info["start_date"],
                                global_prices_info["end_date"],
                            )
                            dataset = hapi_dataset_generator.generate_prices_dataset(
                                hapi_currencies,
                                hapi_commodities,
                                hapi_markets,
                                hapi_prices,
                            )
                            if dataset:
                                dataset.update_from_yaml(
                                    script_dir_plus_file(
                                        join(
                                            "config",
                                            "hdx_hapi_dataset_static.yaml",
                                        ),
                                        main,
                                    )
                                )
                                dataset.create_in_hdx(
                                    remove_additional_resources=False,
                                    hxl_update=False,
                                    updated_by_script=updated_by_script,
                                    batch=batch,
                                )
                                logger.info("WFP global HAPI dataset created")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
