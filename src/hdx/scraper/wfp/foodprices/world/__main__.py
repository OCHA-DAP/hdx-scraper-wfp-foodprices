#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.data.user import User
from hdx.facades.infer_arguments import facade
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices._version import __version__
from hdx.scraper.wfp.foodprices.utilities import get_currencies, get_now
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.scraper.wfp.foodprices.world.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.world.file_reader import FileReader
from hdx.scraper.wfp.foodprices.world.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.wfp.foodprices.world.hapi_output import HAPIOutput
from hdx.utilities.downloader import Download
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import (
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
    err_to_hdx: bool = False,
) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save all downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.
        countryiso3s (str): Whether to limit to specific countries. Defaults to not limiting ("").
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
                with temp_dir_batch(
                    lookup,
                    delete_if_exists=False,
                    delete_on_success=True,
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
                    wfp_api = WFPAPI(token_downloader, retriever)
                    wfp_api.update_retry_params(attempts=5, wait=3600)
                    wfp_mapping = WFPMappings(configuration, wfp_api, retriever)
                    _, commodities = wfp_mapping.build_commodity_category_mapping()
                    currencies = get_currencies(wfp_api)
                    dataset_generator = DatasetGenerator(
                        configuration,
                        folder,
                        currencies,
                    )

                    file_reader = FileReader(downloader, folder)
                    global_prices_info = file_reader.get_global_prices()
                    if not global_prices_info:
                        logger.error("No prices data found!")
                        return
                    markets = file_reader.get_global_markets()
                    if not markets:
                        logger.error("No markets data found!")
                        return
                    dataset, showcase = (
                        dataset_generator.generate_global_dataset_and_showcase(
                            global_prices_info, markets, commodities
                        )
                    )
                    snippet = "Countries, Commodities and Markets data"
                    dataset.update_from_yaml(
                        script_dir_plus_file(
                            join("config", "hdx_dataset_static.yaml"), get_now
                        )
                    )
                    dataset["notes"] = dataset["notes"] % snippet
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        match_resource_order=True,
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
                        elif resource_name == dataset_generator.global_commodities_name:
                            commodities_resource_id = resource["id"]
                        elif resource_name == dataset_generator.global_currencies_name:
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
                            commodities,
                            dataset_id,
                            commodities_resource_id,
                        )
                        hapi_markets = hapi_output.process_markets(
                            markets, dataset_id, markets_resource_id
                        )
                        hapi_prices_by_year = hapi_output.process_prices(
                            global_prices_info, dataset_id, prices_resource_id
                        )
                        hapi_dataset_generator = HAPIDatasetGenerator(
                            configuration,
                            folder,
                            global_prices_info["start_date"],
                            global_prices_info["end_date"],
                        )
                        dataset = hapi_dataset_generator.generate_prices_dataset(
                            hapi_prices_by_year,
                            hapi_markets,
                            hapi_commodities,
                            hapi_currencies,
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
