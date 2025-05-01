#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os import getenv
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices._version import __version__
from hdx.scraper.wfp.foodprices.country.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.country.wfp_food import WFPFood
from hdx.scraper.wfp.foodprices.utilities import get_now, setup_currency
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    temp_dir_batch,
)
from hdx.utilities.retriever import Retrieve

setup_logging()
logger = logging.getLogger(__name__)

lookup = "hdx-scraper-wfp-foodprices"
updated_by_script = "HDX Scraper: WFP Food Prices"


def main(
    save: bool = False,
    use_saved: bool = False,
    countryiso3s: str = "",
    save_wfp_rates: bool = True,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save all downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        countryiso3s (str): Whether to limit to specific countries. Defaults to not limiting ("").
        save_wfp_rates (bool): Save WFP FX rates data. Defaults to True.

    Returns:
        None
    """
    logger.info(f"##### {lookup} version {__version__} ####")
    if not User.check_current_user_organization_access(
        "3ecac442-7fed-448d-8f78-b385ef6f84e7", "create_dataset"
    ):
        raise PermissionError("API Token does not give access to WFP organisation!")
    with Download(
        fail_on_missing_file=False,
        extra_params_yaml=join(expanduser("~"), ".extraparams.yaml"),
        extra_params_lookup=lookup,
    ) as token_downloader:
        with Download(
            use_env=False, rate_limit={"calls": 1, "period": 0.1}
        ) as downloader:
            delete_if_exists = False
            wheretostart = getenv("WHERETOSTART")
            if wheretostart:
                if wheretostart.upper() == "RESET":
                    delete_if_exists = True
                    logger.info("Removing progress file and will start from beginning!")
            with temp_dir_batch(
                lookup,
                delete_if_exists,
                delete_on_success=False,
                delete_on_failure=False,
            ) as info:
                if countryiso3s:
                    countryiso3s = countryiso3s.split(",")
                else:
                    countryiso3s = None
                folder = info["folder"]
                batch = info["batch"]
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                configuration = Configuration.read()
                base_configuration = script_dir_plus_file(
                    join("config", "project_configuration.yaml"), get_now
                )
                configuration.update(load_yaml(base_configuration))
                now = get_now(retriever)
                wfp_api = WFPAPI(token_downloader, retriever)
                wfp_api.update_retry_params(attempts=5, wait=3600)
                wfp_mapping = WFPMappings(configuration, wfp_api, retriever)
                iso3_to_showcase_url = wfp_mapping.read_region_mapping()
                iso3_to_source = wfp_mapping.read_source_overrides()
                countries = wfp_mapping.get_countries(countryiso3s)
                logger.info(f"Number of country datasets to upload: {len(countries)}")
                commodity_to_category, _ = (
                    wfp_mapping.build_commodity_category_mapping()
                )
                if save_wfp_rates:
                    wfp_rates_folder = folder
                else:
                    wfp_rates_folder = None
                currencies = setup_currency(now, retriever, wfp_api, wfp_rates_folder)
                dataset_generator = DatasetGenerator(
                    configuration,
                    folder,
                    iso3_to_showcase_url,
                    iso3_to_source,
                    currencies,
                )

                for _, country in progress_storing_folder(info, countries, "iso3"):
                    countryiso3 = country["iso3"]
                    dataset, showcase = dataset_generator.get_dataset_and_showcase(
                        countryiso3
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
                    success = wfp_food.get_price_markets(wfp_api)
                    if not success:
                        return
                    prices_info, markets, market_to_commodities, sources = (
                        wfp_food.generate_rows()
                    )
                    dataset, qc_indicators = dataset_generator.complete_dataset(
                        countryiso3,
                        dataset,
                        prices_info,
                        markets,
                        market_to_commodities,
                        sources,
                    )

                    snippet = f"Food Prices data for {country['name']}"
                    if dataset:
                        dataset.update_from_yaml(
                            script_dir_plus_file(
                                join("config", "hdx_dataset_static.yaml"),
                                get_now,
                            )
                        )
                        dataset["notes"] = dataset["notes"] % snippet
                        dataset.generate_quickcharts(-1, indicators=qc_indicators)
                        dataset.create_in_hdx(
                            remove_additional_resources=True,
                            match_resource_order=True,
                            hxl_update=False,
                            updated_by_script=updated_by_script,
                            batch=batch,
                        )
                        if showcase:
                            showcase.create_in_hdx()
                            showcase.add_dataset(dataset)
                        else:
                            logger.info(f"{country['name']} does not have a showcase!")


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
