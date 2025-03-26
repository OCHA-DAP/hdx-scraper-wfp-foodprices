#!/usr/bin/python
"""
Unit tests for wfpfood scraper.

"""

import logging
from os import remove
from os.path import join

from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.database import Database
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.db_updater import DBUpdater
from hdx.scraper.wfp.foodprices.global_prices import get_global_prices_rows
from hdx.scraper.wfp.foodprices.hapi_dataset_generator import HAPIDatasetGenerator
from hdx.scraper.wfp.foodprices.hapi_output import HAPIOutput
from hdx.scraper.wfp.foodprices.utilities import get_now, setup_currency
from hdx.scraper.wfp.foodprices.wfp_food import WFPFood
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class TestWFP:
    def test_run(self, configuration, fixtures_dir, input_dir):
        with HDXErrorHandler() as error_handler:
            with temp_dir(
                "TestWFPFoodPrices",
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
                    dbpath = f"/{tempdir}/foodprices.sqlite"
                    try:
                        remove(dbpath)
                    except OSError:
                        pass
                    params = {
                        "dialect": "sqlite",
                        "database": dbpath,
                    }
                    with Database(**params) as database:
                        now = get_now(retriever)
                        wfp_api = WFPAPI(downloader, retriever)
                        wfp = WFPMappings(configuration, wfp_api, retriever)
                        iso3_to_showcase_url = wfp.read_region_mapping()
                        assert len(iso3_to_showcase_url) == 88
                        iso3_to_source = wfp.read_source_overrides()
                        assert len(iso3_to_source) == 24
                        countries = wfp.get_countries()
                        assert len(countries) == 291
                        assert countries[100:102] == [
                            {"iso3": "GTM", "name": "Guatemala"},
                            {"iso3": "GUF", "name": "French Guiana"},
                        ]
                        commodity_to_category, dbcommodities = (
                            wfp.build_commodity_category_mapping()
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
                        dbupdater = DBUpdater(configuration, database)
                        dbupdater.update_commodities(dbcommodities)

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
                        dbmarkets = wfp_food.get_price_markets(wfp_api)
                        rows, markets, sources = wfp_food.generate_rows(dbmarkets)
                        dataset, qc_indicators = dataset_generator.complete_dataset(
                            countryiso3, dataset, rows, markets, sources
                        )

                        time_period = dataset.get_time_period()
                        hdx_url = dataset.get_hdx_url()
                        dbupdater.update_tables(
                            countryiso3, time_period, hdx_url, dbmarkets
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
                            "dataset_source": "CARITAS, GOV, Gvt, National Institute Of Statistics (INS), WFP",
                            "dataset_date": "[2011-01-15T00:00:00 TO 2023-10-15T23:59:59]",
                        }
                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "name": "Congo - Food Prices",
                                "description": "Food prices data with HXL tags",
                                "format": "csv",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "name": "QuickCharts: Congo - Food Prices",
                                "description": "Food prices QuickCharts data with HXL tags",
                                "format": "csv",
                                "resource_type": "file.upload",
                                "url_type": "upload",
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
                        assert qc_indicators == [
                            {
                                "code": "Brazzaville-Brazzaville-Total-Beans (white)-KG-Retail-XAF",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Beans (white) ($/KG) in Brazzaville/Total",
                                "title": "Price of Beans (white) in Total",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                            {
                                "code": "Point-Noire-Pointe-Noire-Grand marché/Fond Ntié-Ntié/Nkouikou-Rice "
                                "(mixed, low quality)-KG-Retail-XAF",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Rice (mixed, low quality) ($/KG) in "
                                "Point-Noire/Pointe-Noire/Grand marché/Fond "
                                "Ntié-Ntié/Nkouikou",
                                "title": "Price of Rice (mixed, low quality) in Grand marché/Fond "
                                "Ntié-Ntié/Nkouikou",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                            {
                                "code": "Pool-Kinkala-Kinkala-Oil (vegetable)-L-Retail-XAF",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Oil (vegetable) ($/L) in Pool/Kinkala",
                                "title": "Price of Oil (vegetable) in Kinkala",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                        ]

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
                        dbmarkets = wfp_food.get_price_markets(wfp_api)
                        rows, markets, sources = wfp_food.generate_rows(dbmarkets)
                        dataset, qc_indicators = dataset_generator.complete_dataset(
                            countryiso3, dataset, rows, markets, sources
                        )

                        time_period = dataset.get_time_period()
                        hdx_url = dataset.get_hdx_url()
                        dbupdater.update_tables(
                            countryiso3, time_period, hdx_url, dbmarkets
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
                            "dataset_source": "FPMA, National Statistical Committee of the Republic of Belarus via FAO: GIEWS",
                            "dataset_date": "[2009-01-15T00:00:00 TO 2022-05-15T23:59:59]",
                        }
                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "name": "Belarus - Food Prices",
                                "description": "Food prices data with HXL tags",
                                "format": "csv",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "name": "QuickCharts: Belarus - Food Prices",
                                "description": "Food prices QuickCharts data with HXL tags",
                                "format": "csv",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                        ]
                        assert showcase is None
                        assert qc_indicators == [
                            {
                                "code": "Minsk City-Minsk City-Minsk-Wheat flour-KG-Retail-BYR",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Wheat flour ($/KG) in Minsk City/Minsk",
                                "title": "Price of Wheat flour in Minsk",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            }
                        ]

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
                        dbmarkets = wfp_food.get_price_markets(wfp_api)
                        rows, markets, sources = wfp_food.generate_rows(dbmarkets)
                        dataset, qc_indicators = dataset_generator.complete_dataset(
                            countryiso3, dataset, rows, markets, sources
                        )

                        time_period = dataset.get_time_period()
                        hdx_url = dataset.get_hdx_url()
                        dbupdater.update_tables(
                            countryiso3, time_period, hdx_url, dbmarkets
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
                            "dataset_source": "FPMA, PCBS, Palestinian Central Bureau of Statistics via FAO: GIEWS, VAM",
                            "dataset_date": "[2007-01-15T00:00:00 TO 2024-01-15T23:59:59]",
                        }
                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "name": "State of Palestine - Food Prices",
                                "description": "Food prices data with HXL tags",
                                "format": "csv",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "name": "QuickCharts: State of Palestine - Food Prices",
                                "description": "Food prices QuickCharts data with HXL tags",
                                "format": "csv",
                                "resource_type": "file.upload",
                                "url_type": "upload",
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
                        assert qc_indicators == [
                            {
                                "code": "Gaza Strip-Gaza-Gaza-Oil (olive)-KG-Retail-ILS",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Oil (olive) ($/KG) in Gaza Strip/Gaza",
                                "title": "Price of Oil (olive) in Gaza",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                            {
                                "code": "West Bank-Tulkarm-Tulkarem-Water (drinking)-Cubic meter-Retail-ILS",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Water (drinking) ($/Cubic meter) in West "
                                "Bank/Tulkarm/Tulkarem",
                                "title": "Price of Water (drinking) in Tulkarem",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                            {
                                "code": "West Bank-Ramallah and Albireh-Ramallah-Tea-KG-Retail-ILS",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Tea ($/KG) in West Bank/Ramallah and "
                                "Albireh/Ramallah",
                                "title": "Price of Tea in Ramallah",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                        ]

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
                        dbmarkets = wfp_food.get_price_markets(wfp_api)
                        rows, markets, sources = wfp_food.generate_rows(dbmarkets)
                        dataset, qc_indicators = dataset_generator.complete_dataset(
                            countryiso3, dataset, rows, markets, sources
                        )

                        time_period = dataset.get_time_period()
                        hdx_url = dataset.get_hdx_url()
                        dbupdater.update_tables(
                            countryiso3, time_period, hdx_url, dbmarkets
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
                            "title": "Syrian Arab Republic - Food Prices",
                        }
                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "description": "Food prices data with HXL tags",
                                "format": "csv",
                                "name": "Syrian Arab Republic - Food Prices",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "description": "Food prices QuickCharts data with HXL tags",
                                "format": "csv",
                                "name": "QuickCharts: Syrian Arab Republic - Food Prices",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                        ]
                        assert showcase == {
                            "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                            "name": "wfp-food-prices-for-syrian-arab-republic-showcase",
                            "notes": "Syrian Arab Republic food prices data from World Food Programme "
                            "displayed through VAM Economic Explorer",
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
                            "title": "Syrian Arab Republic - Food Prices showcase",
                            "url": "https://dataviz.vam.wfp.org/the-middle-east-and-northern-africa/syrian-arab-republic/overview",
                        }
                        assert qc_indicators == [
                            {
                                "code": "Lattakia-Jablah-Jablah-Wheat flour-KG-Retail-SYP",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Wheat flour ($/KG) in Lattakia/Jablah",
                                "title": "Price of Wheat flour in Jablah",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                            {
                                "code": "Tartous-Tartous-Tartous-Oil-L-Retail-SYP",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Oil ($/L) in Tartous",
                                "title": "Price of Oil in Tartous",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                            {
                                "code": "Rural Damascus-Rural Damascus-Qudsiya-Beans (white)-KG-Retail-SYP",
                                "code_col": "#meta+code",
                                "date_col": "#date",
                                "description": "Price of Beans (white) ($/KG) in Rural Damascus/Qudsiya",
                                "title": "Price of Beans (white) in Qudsiya",
                                "unit": "US Dollars ($)",
                                "value_col": "#value+usd",
                            },
                        ]

                        table_data, start_date, end_date = (
                            dbupdater.get_data_from_tables()
                        )
                        global_prices_info = get_global_prices_rows(downloader, tempdir)
                        dataset, showcase = (
                            dataset_generator.generate_global_dataset_and_showcase(
                                global_prices_info, table_data, start_date, end_date
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
                            "dataset_date": "[2020-05-15T00:00:00 TO 2024-01-15T23:59:59]",
                        }
                        resources = dataset.get_resources()
                        assert resources == [
                            {
                                "description": "Last 2 years (per country) of prices data with HXL tags",
                                "format": "csv",
                                "name": "Global WFP food prices",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "description": "Countries data with HXL tags with links to country datasets containing all available historic data (2007-01-15 to 2024-01-15)",
                                "format": "csv",
                                "name": "Global WFP countries",
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
                            error_handler,
                        )
                        hapi_output.setup_admins(retriever)
                        global_markets_info = table_data["DBMarket"]
                        hapi_output.process_markets(global_markets_info, "1234", "5678")
                        hapi_output.process_prices(
                            global_prices_info, "1234", "9101112"
                        )
                        hapi_dataset_generator = HAPIDatasetGenerator(
                            configuration,
                            global_markets_info,
                            global_prices_info,
                        )
                        dataset = hapi_dataset_generator.generate_prices_dataset(
                            tempdir,
                        )
                        assert dataset == {
                            "data_update_frequency": "7",
                            "dataset_date": "[2020-05-15T00:00:00 TO 2024-01-15T23:59:59]",
                            "dataset_preview": "no_preview",
                            "dataset_source": "WFP - World Food Programme",
                            "groups": [{"name": "world"}],
                            "license_id": "cc-by-igo",
                            "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                            "name": "hdx-hapi-humanitarian-needs",
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
                                "description": "Food Prices & Market Monitor data from HDX HAPI, please see "
                                "[the "
                                "documentation](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/#food-prices-market-monitor) "
                                "for more information",
                                "format": "csv",
                                "name": "Global Food Security, Nutrition & Poverty: Food Prices",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                            {
                                "dataset_preview_enabled": "False",
                                "description": "Food Markets data",
                                "format": "csv",
                                "name": "Global Food Security, Nutrition & Poverty: Food Markets",
                                "resource_type": "file.upload",
                                "url_type": "upload",
                            },
                        ]

                        for filename in (
                            "wfp_food_prices_cog",
                            "wfp_food_prices_cog_qc",
                            "wfp_food_prices_blr",
                            "wfp_food_prices_blr_qc",
                            "wfp_food_prices_pse",
                            "wfp_food_prices_pse_qc",
                            "wfp_food_prices_syr",
                            "wfp_food_prices_syr_qc",
                            "wfp_food_prices_global",
                            "wfp_commodities_global",
                            "wfp_countries_global",
                            "wfp_markets_global",
                            "wfp_currencies_global",
                            "hdx_hapi_food_price_global",
                            "hdx_hapi_food_market_global",
                        ):
                            csv_filename = f"{filename}.csv"
                            expected_file = join(fixtures_dir, csv_filename)
                            actual_file = join(tempdir, csv_filename)
                            logger.info(f"Comparing {actual_file} with {expected_file}")
                            assert_files_same(expected_file, actual_file)
