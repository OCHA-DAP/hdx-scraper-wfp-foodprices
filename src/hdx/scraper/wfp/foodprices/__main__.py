#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.database import Database
from hdx.facades.keyword_arguments import facade
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.wfpfood import WFPFood
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
                    wfp = WFPFood(
                        configuration, folder, wfp_api, retriever, session
                    )
                    wfp.read_region_mapping()
                    wfp.read_source_overrides()
                    countries = wfp.get_countries()
                    logger.info(
                        f"Number of country datasets to upload: {len(countries)}"
                    )
                    wfp.build_mappings()

                    def process_country(country):
                        countryiso3 = country["iso3"]
                        (
                            dataset,
                            showcase,
                            qc_indicators,
                        ) = wfp.generate_dataset_and_showcase(countryiso3)
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
                        wfp.update_database()

                    for _, country in progress_storing_folder(
                        info, countries, "iso3"
                    ):
                        process_country(country)
                    dataset, showcase = (
                        wfp.generate_global_dataset_and_showcase()
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
