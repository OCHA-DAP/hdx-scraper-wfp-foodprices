#!/usr/bin/python
"""
Unit tests for wfpfood scraper.

"""

import gc
import logging
from datetime import datetime, timezone
from os.path import join

import pytest

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.utilities import get_currencies
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.scraper.wfp.foodprices.world.__main__ import main
from hdx.scraper.wfp.foodprices.world.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.world.global_markets import get_markets
from hdx.scraper.wfp.foodprices.world.global_prices_generator import (
    GlobalPricesGenerator,
)
from hdx.scraper.wfp.foodprices.world.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.wfp.foodprices.world.hapi_output import HAPIOutput
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class TestWFP:
    @pytest.fixture(scope="class")
    def global_dir(self, fixtures_dir):
        return join(fixtures_dir, "global")

    def test_run(self, configuration, global_dir, input_dir, country_dir):
        global_configuration = script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        )
        configuration.update(load_yaml(global_configuration))
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestWFPFoodPricesGlobal",
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
                    wfp_api = WFPAPI(downloader, retriever)
                    wfp = WFPMappings(configuration, wfp_api, retriever)
                    _, commodities = wfp.build_commodity_category_mapping()
                    assert len(commodities) == 1072
                    currencies = get_currencies(wfp_api)
                    assert len(currencies) == 127
                    markets = get_markets(downloader, country_dir)
                    assert len(markets) == 205

                    prices_generator = GlobalPricesGenerator(
                        configuration, downloader, country_dir
                    )
                    start_date, end_date = prices_generator.get_years_per_country()
                    assert start_date == datetime(
                        2000, 1, 15, 0, 0, tzinfo=timezone.utc
                    )
                    assert end_date == datetime(2024, 11, 15, 0, 0, tzinfo=timezone.utc)
                    year_to_pricespath = prices_generator.create_prices_files(tempdir)
                    assert year_to_pricespath == {
                        2000: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2000.csv",
                        2001: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2001.csv",
                        2002: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2002.csv",
                        2003: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2003.csv",
                        2004: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2004.csv",
                        2005: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2005.csv",
                        2006: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2006.csv",
                        2007: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2007.csv",
                        2008: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2008.csv",
                        2009: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2009.csv",
                        2010: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2010.csv",
                        2011: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2011.csv",
                        2012: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2012.csv",
                        2013: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2013.csv",
                        2014: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2014.csv",
                        2015: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2015.csv",
                        2016: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2016.csv",
                        2017: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2017.csv",
                        2018: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2018.csv",
                        2019: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2019.csv",
                        2020: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2020.csv",
                        2021: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2021.csv",
                        2022: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2022.csv",
                        2023: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2023.csv",
                        2024: "/tmp/TestWFPFoodPricesGlobal/wfp_food_prices_global_2024.csv",
                    }

                    dataset_generator = DatasetGenerator(
                        configuration, tempdir, start_date, end_date
                    )

                    dataset, showcase = (
                        dataset_generator.generate_global_dataset_and_showcase(
                            year_to_pricespath, markets, commodities, currencies
                        )
                    )
                    logger.info("Generated global")
                    assert dataset == {
                        "name": "global-wfp-food-prices",
                        "title": "Global - Food Prices",
                        "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                        "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                        "data_update_frequency": "30",
                        "groups": [{"name": "world"}],
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
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
                        "dataset_source": "WFP",
                        "dataset_date": "[2000-01-15T00:00:00 TO 2024-11-15T23:59:59]",
                    }
                    resources = dataset.get_resources()
                    assert resources == [
                        {
                            "description": "Prices data for 2024 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2024",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2023 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2023",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2022 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2022",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2021 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2021",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2020 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2020",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2019 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2019",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2018 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2018",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2017 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2017",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2016 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2016",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2015 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2015",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2014 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2014",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2013 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2013",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2012 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2012",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2011 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2011",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2010 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2010",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2009 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2009",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2008 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2008",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2007 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2007",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2006 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2006",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2005 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2005",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2004 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2004",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2003 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2003",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2002 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2002",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2001 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2001",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Prices data for 2000 with HXL tags",
                            "format": "csv",
                            "name": "Global WFP food prices 2000",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Commodities data with HXL tags",
                            "format": "csv",
                            "name": "Global WFP commodities",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Markets data with HXL tags",
                            "format": "csv",
                            "name": "Global WFP markets",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "description": "Currencies data with HXL tags",
                            "format": "csv",
                            "name": "Global WFP currencies",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                    ]
                    assert showcase == {
                        "name": "global-wfp-food-prices-showcase",
                        "title": "Global - Food Prices showcase",
                        "notes": "Global food prices data from World Food Programme displayed through VAM Economic Explorer",
                        "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                        "url": "https://dataviz.vam.wfp.org/economic/prices",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
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

                    hapi_output = HAPIOutput(
                        configuration,
                        downloader,
                        tempdir,
                        error_handler,
                    )
                    hapi_output.setup_admins(retriever)
                    hapi_currencies = hapi_output.process_currencies(
                        currencies, "1234", "abcd"
                    )
                    hapi_commodities = hapi_output.process_commodities(
                        commodities,
                        "1234",
                        "efgh",
                    )
                    hapi_markets = hapi_output.process_markets(markets, "1234", "5678")
                    year_to_prices_resource_id = {
                        year: "9101112" for year in range(2000, 2025)
                    }
                    hapi_year_to_pricespath = hapi_output.create_prices_files(
                        year_to_pricespath, "1234", year_to_prices_resource_id, tempdir
                    )
                    assert hapi_year_to_pricespath == {
                        2000: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2000.csv",
                        2001: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2001.csv",
                        2002: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2002.csv",
                        2003: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2003.csv",
                        2004: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2004.csv",
                        2005: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2005.csv",
                        2006: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2006.csv",
                        2007: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2007.csv",
                        2008: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2008.csv",
                        2009: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2009.csv",
                        2010: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2010.csv",
                        2011: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2011.csv",
                        2012: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2012.csv",
                        2013: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2013.csv",
                        2014: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2014.csv",
                        2015: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2015.csv",
                        2016: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2016.csv",
                        2017: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2017.csv",
                        2018: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2018.csv",
                        2019: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2019.csv",
                        2020: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2020.csv",
                        2021: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2021.csv",
                        2022: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2022.csv",
                        2023: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2023.csv",
                        2024: "/tmp/TestWFPFoodPricesGlobal/hdx_hapi_food_price_global_2024.csv",
                    }

                    hapi_dataset_generator = HAPIDatasetGenerator(
                        configuration,
                        tempdir,
                        start_date,
                        end_date,
                    )
                    dataset = hapi_dataset_generator.generate_prices_dataset(
                        hapi_year_to_pricespath,
                        hapi_markets,
                        hapi_commodities,
                        hapi_currencies,
                    )

                    assert dataset == {
                        "data_update_frequency": "30",
                        "dataset_date": "[2000-01-15T00:00:00 TO 2024-11-15T23:59:59]",
                        "dataset_preview": "no_preview",
                        "dataset_source": "WFP - World Food Programme",
                        "groups": [{"name": "world"}],
                        "license_id": "cc-by-igo",
                        "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                        "name": "hdx-hapi-food-price",
                        "owner_org": "40d10ece-49de-4791-9aed-e164f1d16dd1",
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "economics",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "food security",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "title": "HDX HAPI - Food Security, Nutrition & Poverty: Food Prices",
                    }
                    resources = dataset.get_resources()
                    assert resources == [
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2024 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2024",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2023 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2023",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2022 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2022",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2021 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2021",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2020 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2020",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2019 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2019",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2018 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2018",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2017 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2017",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2016 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2016",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2015 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2015",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2014 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2014",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2013 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2013",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2012 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2012",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2011 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2011",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2010 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2010",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2009 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2009",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2008 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2008",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2007 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2007",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2006 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2006",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2005 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2005",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2004 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2004",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2003 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2003",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2002 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2002",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2001 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2001",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "2000 Food Prices & Market Monitor data from HDX HAPI, please "
                            "see [the "
                            "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                            "for more information",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Food Prices 2000",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "Markets data",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Markets",
                            "p_coded": True,
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "Commodities data",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Commodities",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                        {
                            "dataset_preview_enabled": "False",
                            "description": "Currencies data",
                            "format": "csv",
                            "name": "Global Food Security, Nutrition & Poverty: Currencies",
                            "resource_type": "file.upload",
                            "url_type": "upload",
                        },
                    ]

                    for filename in (
                        "wfp_commodities_global",
                        "wfp_markets_global",
                        "wfp_currencies_global",
                        "hdx_hapi_market_global",
                        "hdx_hapi_commodity_global",
                        "hdx_hapi_currency_global",
                    ):
                        csv_filename = f"{filename}.csv"
                        expected_file = join(global_dir, csv_filename)
                        actual_file = join(tempdir, csv_filename)
                        logger.info(f"Comparing {actual_file} with {expected_file}")
                        assert_files_same(expected_file, actual_file)

                    for year in range(2000, 2025):
                        for filename in (
                            f"wfp_food_prices_global_{year}",
                            f"hdx_hapi_food_price_global_{year}",
                        ):
                            csv_filename = f"{filename}.csv"
                            expected_file = join(global_dir, csv_filename)
                            actual_file = join(tempdir, csv_filename)
                            logger.info(f"Comparing {actual_file} with {expected_file}")
                            assert_files_same(expected_file, actual_file)
                            gc.collect()
