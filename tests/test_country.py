#!/usr/bin/python
"""
Unit tests for wfpfood scraper.

"""

import gc
import logging
from os.path import join

from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.country.__main__ import main
from hdx.scraper.wfp.foodprices.country.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.country.wfp_food import WFPFood
from hdx.scraper.wfp.foodprices.utilities import get_now, setup_currency
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class TestWFP:
    def test_run(self, configuration, country_dir, input_dir):
        country_configuration = script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        )
        configuration.update(load_yaml(country_configuration))
        with temp_dir(
            "TestWFPFoodPricesCountry",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            logger.info("Starting")
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader,
                    tempdir,
                    input_dir,
                    tempdir,
                    save=False,
                    use_saved=True,
                )
                now = get_now(retriever)
                wfp_api = WFPAPI(downloader, retriever)
                wfp_mapping = WFPMappings(configuration, wfp_api, retriever)
                iso3_to_showcase_url = wfp_mapping.read_region_mapping()
                assert len(iso3_to_showcase_url) == 88
                iso3_to_source = wfp_mapping.read_source_overrides()
                assert len(iso3_to_source) == 24
                countries = wfp_mapping.get_countries()
                assert len(countries) == 291
                assert countries[100:102] == [
                    {"iso3": "GTM", "name": "Guatemala"},
                    {"iso3": "GUF", "name": "French Guiana"},
                ]
                commodity_to_category, _ = (
                    wfp_mapping.build_commodity_category_mapping()
                )
                assert len(commodity_to_category) == 1072

                currencies = setup_currency(now, retriever, wfp_api, input_dir)
                assert len(currencies) == 127
                dataset_generator = DatasetGenerator(
                    configuration,
                    tempdir,
                    iso3_to_showcase_url,
                    iso3_to_source,
                    currencies,
                )

                countryiso3 = "COG"
                dataset, showcase = dataset_generator.get_dataset_and_showcase(
                    countryiso3
                )
                wfp_food = WFPFood(
                    countryiso3,
                    configuration,
                    iso3_to_showcase_url.get(countryiso3),
                    iso3_to_source.get(countryiso3),
                    commodity_to_category,
                )
                _ = wfp_food.get_price_markets(wfp_api)
                prices, markets, sources = wfp_food.generate_rows()
                dataset = dataset_generator.complete_dataset(
                    countryiso3,
                    dataset,
                    prices,
                    markets,
                    sources,
                )

                logger.info("Generated COG")
                assert dataset == {
                    "name": "wfp-food-prices-for-congo",
                    "title": "Congo - Food Prices",
                    "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                    "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                    "data_update_frequency": "30",
                    "groups": [{"name": "cog"}],
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "dataset_source": "CARITAS, GOV, Gvt, National Institute Of Statistics (INS), WFP",
                    "dataset_date": "[2011-01-15T00:00:00 TO 2023-10-15T23:59:59]",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Food prices data",
                        "format": "csv",
                        "name": "Congo - Food Prices",
                    },
                    {
                        "description": "Markets data",
                        "format": "csv",
                        "name": "Congo - Markets",
                    },
                ]
                assert showcase == {
                    "name": "wfp-food-prices-for-congo-showcase",
                    "title": "Congo - Food Prices showcase",
                    "notes": "Congo food prices data from World Food Programme displayed through VAM Economic Explorer",
                    "url": "https://dataviz.vam.wfp.org/southern-africa/congo/overview",
                    "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                }
                countryiso3 = "NIC"
                dataset, showcase = dataset_generator.get_dataset_and_showcase(
                    countryiso3
                )
                wfp_food = WFPFood(
                    countryiso3,
                    configuration,
                    iso3_to_showcase_url.get(countryiso3),
                    iso3_to_source.get(countryiso3),
                    commodity_to_category,
                )
                _ = wfp_food.get_price_markets(wfp_api)
                prices, markets, sources = wfp_food.generate_rows()
                dataset = dataset_generator.complete_dataset(
                    countryiso3,
                    dataset,
                    prices,
                    markets,
                    sources,
                )

                logger.info("Generated NIC")
                assert dataset == {
                    "data_update_frequency": "30",
                    "dataset_date": "[2000-01-15T00:00:00 TO 2024-11-15T23:59:59]",
                    "dataset_source": "Banco Central de Nicaragua, CAC, FPMA, Government of "
                    "Nicaragua, INE (Instituto Nicaragüense de Energía), INIDE, "
                    "Insitituto Nacional de Información de Desarrollo, "
                    "Instituto Nicaraguense de Energia, Ministerio Agropecuario "
                    "y Forestal via FAO: GIEWS, SIMPAH via FAO: GIEWS",
                    "groups": [{"name": "nic"}],
                    "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                    "name": "wfp-food-prices-for-nicaragua",
                    "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Nicaragua - Food Prices",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Food prices data",
                        "format": "csv",
                        "name": "Nicaragua - Food Prices",
                    },
                    {
                        "description": "Markets data",
                        "format": "csv",
                        "name": "Nicaragua - Markets",
                    },
                ]
                assert showcase == {
                    "name": "wfp-food-prices-for-nicaragua-showcase",
                    "title": "Nicaragua - Food Prices showcase",
                    "notes": "Nicaragua food prices data from World Food Programme displayed through VAM Economic Explorer",
                    "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                    "url": "https://dataviz.vam.wfp.org/latin-america-and-the-caribbean/nicaragua/overview",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                }

                countryiso3 = "BLR"
                dataset, showcase = dataset_generator.get_dataset_and_showcase(
                    countryiso3
                )
                wfp_food = WFPFood(
                    countryiso3,
                    configuration,
                    iso3_to_showcase_url.get(countryiso3),
                    iso3_to_source.get(countryiso3),
                    commodity_to_category,
                )
                _ = wfp_food.get_price_markets(wfp_api)
                prices, markets, sources = wfp_food.generate_rows()
                dataset = dataset_generator.complete_dataset(
                    countryiso3,
                    dataset,
                    prices,
                    markets,
                    sources,
                )

                logger.info("Generated BLR")
                assert dataset == {
                    "name": "wfp-food-prices-for-belarus",
                    "title": "Belarus - Food Prices",
                    "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                    "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                    "data_update_frequency": "30",
                    "groups": [{"name": "blr"}],
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "dataset_source": "FPMA, National Statistical Committee of the Republic of Belarus via FAO: GIEWS",
                    "dataset_date": "[2009-01-15T00:00:00 TO 2022-05-15T23:59:59]",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Food prices data",
                        "format": "csv",
                        "name": "Belarus - Food Prices",
                    },
                    {
                        "description": "Markets data",
                        "format": "csv",
                        "name": "Belarus - Markets",
                    },
                ]
                assert showcase is None

                countryiso3 = "PSE"
                dataset, showcase = dataset_generator.get_dataset_and_showcase(
                    countryiso3
                )
                wfp_food = WFPFood(
                    countryiso3,
                    configuration,
                    iso3_to_showcase_url.get(countryiso3),
                    iso3_to_source.get(countryiso3),
                    commodity_to_category,
                )
                _ = wfp_food.get_price_markets(wfp_api)
                prices, markets, sources = wfp_food.generate_rows()
                dataset = dataset_generator.complete_dataset(
                    countryiso3,
                    dataset,
                    prices,
                    markets,
                    sources,
                )

                logger.info("Generated PSE")
                assert dataset == {
                    "name": "wfp-food-prices-for-state-of-palestine",
                    "title": "State of Palestine - Food Prices",
                    "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                    "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                    "data_update_frequency": "30",
                    "groups": [{"name": "pse"}],
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "dataset_source": "FPMA, PCBS, Palestinian Central Bureau of Statistics via FAO: GIEWS, VAM",
                    "dataset_date": "[2007-01-15T00:00:00 TO 2024-01-15T23:59:59]",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Food prices data",
                        "format": "csv",
                        "name": "State of Palestine - Food Prices",
                    },
                    {
                        "description": "Markets data",
                        "format": "csv",
                        "name": "State of Palestine - Markets",
                    },
                ]
                assert showcase == {
                    "name": "wfp-food-prices-for-state-of-palestine-showcase",
                    "title": "State of Palestine - Food Prices showcase",
                    "notes": "State of Palestine food prices data from World Food Programme displayed through VAM Economic Explorer",
                    "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                    "url": "https://dataviz.vam.wfp.org/the-middle-east-and-northern-africa/palestine/overview",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                }

                countryiso3 = "SYR"
                dataset, showcase = dataset_generator.get_dataset_and_showcase(
                    countryiso3
                )
                wfp_food = WFPFood(
                    countryiso3,
                    configuration,
                    iso3_to_showcase_url.get(countryiso3),
                    iso3_to_source.get(countryiso3),
                    commodity_to_category,
                )
                _ = wfp_food.get_price_markets(wfp_api)
                prices, markets, sources = wfp_food.generate_rows()
                dataset = dataset_generator.complete_dataset(
                    countryiso3,
                    dataset,
                    prices,
                    markets,
                    sources,
                )

                logger.info("Generated SYR")
                assert dataset == {
                    "data_update_frequency": "30",
                    "dataset_date": "[2011-04-15T00:00:00 TO 2023-12-15T23:59:59]",
                    "dataset_source": "TestSYRSource",
                    "groups": [{"name": "syr"}],
                    "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                    "name": "wfp-food-prices-for-syrian-arab-republic",
                    "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                    "subnational": "1",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Syrian Arab Republic - Food Prices",
                }
                resources = dataset.get_resources()
                assert resources == [
                    {
                        "description": "Food prices data",
                        "format": "csv",
                        "name": "Syrian Arab Republic - Food Prices",
                    },
                    {
                        "description": "Markets data",
                        "format": "csv",
                        "name": "Syrian Arab Republic - Markets",
                    },
                ]
                assert showcase == {
                    "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                    "name": "wfp-food-prices-for-syrian-arab-republic-showcase",
                    "notes": "Syrian Arab Republic food prices data from World Food Programme "
                    "displayed through VAM Economic Explorer",
                    "tags": [
                        {
                            "name": "economics",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "food security",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "indicators",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "markets",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Syrian Arab Republic - Food Prices showcase",
                    "url": "https://dataviz.vam.wfp.org/the-middle-east-and-northern-africa/syrian-arab-republic/overview",
                }

                for filename in (
                    "wfp_food_prices_cog",
                    "wfp_markets_cog",
                    "wfp_food_prices_blr",
                    "wfp_markets_blr",
                    "wfp_food_prices_pse",
                    "wfp_markets_pse",
                    "wfp_food_prices_syr",
                    "wfp_markets_syr",
                ):
                    csv_filename = f"{filename}.csv"
                    expected_file = join(country_dir, csv_filename)
                    actual_file = join(tempdir, csv_filename)
                    logger.info(f"Comparing {actual_file} with {expected_file}")
                    assert_files_same(expected_file, actual_file)
                    gc.collect()
