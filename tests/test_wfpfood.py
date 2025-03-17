#!/usr/bin/python
"""
Unit tests for wfpfood scraper.

"""

import logging
from os import remove
from os.path import join

from hdx.database import Database
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.currency_setup import setup_currency
from hdx.scraper.wfp.foodprices.dataset_generator import DatasetGenerator
from hdx.scraper.wfp.foodprices.db_updater import DBUpdater
from hdx.scraper.wfp.foodprices.wfp_food import WFPFood
from hdx.scraper.wfp.foodprices.wfp_mappings import WFPMappings
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class TestWFP:
    def test_run(self, configuration, fixtures_dir, input_dir):
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

                    setup_currency(retriever, wfp_api)
                    dataset_generator = DatasetGenerator(
                        configuration,
                        tempdir,
                        iso3_to_showcase_url,
                        iso3_to_source,
                    )
                    dbupdater = DBUpdater(configuration, database)
                    dbupdater.update_commodities(dbcommodities)

                    countryiso3 = "COG"
                    dataset, showcase = (
                        dataset_generator.get_dataset_and_showcase(countryiso3)
                    )
                    wfp_food = WFPFood(
                        countryiso3,
                        configuration,
                        iso3_to_showcase_url.get(countryiso3),
                        iso3_to_source.get(countryiso3),
                        commodity_to_category,
                    )
                    wfp_food.get_price_markets(wfp_api)
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
                    dataset, showcase = (
                        dataset_generator.get_dataset_and_showcase(countryiso3)
                    )
                    wfp_food = WFPFood(
                        countryiso3,
                        configuration,
                        iso3_to_showcase_url.get(countryiso3),
                        iso3_to_source.get(countryiso3),
                        commodity_to_category,
                    )
                    wfp_food.get_price_markets(wfp_api)
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
                            "code": "Minsk City-Minsk City-Minsk-Bread (high grade flour)-KG-Retail-BYR",
                            "code_col": "#meta+code",
                            "date_col": "#date",
                            "description": "Price of Bread (high grade flour) ($/KG) in Minsk City/Minsk",
                            "title": "Price of Bread (high grade flour) in Minsk",
                            "unit": "US Dollars ($)",
                            "value_col": "#value+usd",
                        }
                    ]

                    countryiso3 = "PSE"
                    dataset, showcase = (
                        dataset_generator.get_dataset_and_showcase(countryiso3)
                    )
                    wfp_food = WFPFood(
                        countryiso3,
                        configuration,
                        iso3_to_showcase_url.get(countryiso3),
                        iso3_to_source.get(countryiso3),
                        commodity_to_category,
                    )
                    wfp_food.get_price_markets(wfp_api)
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
                    dataset, showcase = (
                        dataset_generator.get_dataset_and_showcase(countryiso3)
                    )
                    wfp_food = WFPFood(
                        countryiso3,
                        configuration,
                        iso3_to_showcase_url.get(countryiso3),
                        iso3_to_source.get(countryiso3),
                        commodity_to_category,
                    )
                    wfp_food.get_price_markets(wfp_api)
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
                    dataset, showcase = (
                        dataset_generator.generate_global_dataset_and_showcase(
                            table_data, start_date, end_date
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
                        "dataset_date": "[2007-01-15T00:00:00 TO 2024-01-15T23:59:59]",
                    }
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
                    for filename in (
                        "wfp_food_prices_cog",
                        "wfp_food_prices_cog_qc",
                        "wfp_food_prices_blr",
                        "wfp_food_prices_blr_qc",
                        "wfp_food_prices_pse",
                        "wfp_food_prices_pse_qc",
                        "wfp_food_prices_syr",
                        "wfp_food_prices_syr_qc",
                        "wfp_commodities_global",
                        "wfp_countries_global",
                        "wfp_markets_global",
                    ):
                        csv_filename = f"{filename}.csv"
                        expected_file = join(fixtures_dir, csv_filename)
                        actual_file = join(tempdir, csv_filename)
                        logger.info(
                            f"Comparing {actual_file} with {expected_file}"
                        )
                        assert_files_same(expected_file, actual_file)
