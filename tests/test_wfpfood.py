#!/usr/bin/python
"""
Unit tests for wfpfood scraper.

"""
from os.path import join

import pytest
from hdx.database import Database
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from wfpfood import WFPFood


class TestWFP:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            hdx_site="prod",
            user_agent="test",
            project_config_yaml=join("tests", "config", "project_configuration.yml"),
        )
        Locations.set_validlocations(
            [
                {"name": "cog", "title": "Congo"},
                {"name": "blr", "title": "Belarus"},
                {"name": "pse", "title": "State of Palestine"},
                {"name": "world", "title": "World"},
            ]
        )
        return Configuration.read()

    @pytest.fixture(scope="class")
    def fixtures_dir(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="class")
    def input_dir(self, fixtures_dir):
        return join(fixtures_dir, "input")

    def test_run(self, configuration, fixtures_dir, input_dir):
        with temp_dir(
            "TestWFPFoodPrices", delete_on_success=True, delete_on_failure=False
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader, tempdir, input_dir, tempdir, save=False, use_saved=True
                )
                params = {
                    "driver": "sqlite",
                    "database": f"/{tempdir}/foodprices.sqlite",
                }
                with Database(**params) as session:
                    wfp = WFPFood(configuration, tempdir, None, retriever, session)
                    countries = wfp.get_countries()
                    assert len(countries) == 291
                    assert countries[100:102] == [
                        {"iso3": "GTM", "name": "Guatemala"},
                        {"iso3": "GUF", "name": "French Guiana"},
                    ]
                    wfp.build_mappings()
                    (
                        dataset,
                        showcase,
                        qc_indicators,
                    ) = wfp.generate_dataset_and_showcase("COG")
                    assert dataset == {
                        "name": "wfp-food-prices-for-congo",
                        "title": "Congo - Food Prices",
                        "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                        "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                        "data_update_frequency": "7",
                        "groups": [{"name": "cog"}],
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "dataset_source": "CARITAS, GOV, Gvt, National Institute Of Statistics (INS), WFP",
                        "dataset_date": "[2011-01-15T00:00:00 TO 2020-07-15T00:00:00]",
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
                        "url": "http://dataviz.vam.wfp.org/economic_explorer/prices?iso3=COG",
                        "image_url": "http://dataviz.vam.wfp.org/_images/home/3_economic.jpg",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                    }
                    assert qc_indicators == [
                        {
                            "code": "Brazzaville-Brazzaville-Total-Beans (white)-KG-XAF",
                            "title": "Price of Beans (white) in Total",
                            "unit": "US Dollars ($)",
                            "description": "Price of Beans (white) ($/KG) in Brazzaville/Total",
                            "code_col": "#meta+code",
                            "value_col": "#value+usd",
                            "date_col": "#date",
                        },
                        {
                            "code": "Point-Noire-Pointe-Noire-Grand marché/Fond Ntié-Ntié/Nkouikou-Rice (mixed, low quality)-KG-XAF",
                            "title": "Price of Rice (mixed, low quality) in Grand marché/Fond Ntié-Ntié/Nkouikou",
                            "unit": "US Dollars ($)",
                            "description": "Price of Rice (mixed, low quality) ($/KG) in Point-Noire/Pointe-Noire/Grand marché/Fond Ntié-Ntié/Nkouikou",
                            "code_col": "#meta+code",
                            "value_col": "#value+usd",
                            "date_col": "#date",
                        },
                        {
                            "code": "Pool-Kinkala-Kinkala-Oil (vegetable)-L-XAF",
                            "title": "Price of Oil (vegetable) in Kinkala",
                            "unit": "US Dollars ($)",
                            "description": "Price of Oil (vegetable) ($/L) in Pool/Kinkala",
                            "code_col": "#meta+code",
                            "value_col": "#value+usd",
                            "date_col": "#date",
                        },
                    ]
                    (
                        dataset,
                        showcase,
                        qc_indicators,
                    ) = wfp.generate_dataset_and_showcase("BLR")
                    assert dataset == {
                        "name": "wfp-food-prices-for-belarus",
                        "title": "Belarus - Food Prices",
                        "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                        "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                        "data_update_frequency": "7",
                        "groups": [{"name": "blr"}],
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "dataset_source": "FPMA, National Statistical Committee of the Republic of Belarus via FAO: GIEWS",
                        "dataset_date": "[2009-01-15T00:00:00 TO 2021-04-15T00:00:00]",
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
                    assert showcase == {
                        "name": "wfp-food-prices-for-belarus-showcase",
                        "title": "Belarus - Food Prices showcase",
                        "notes": "Belarus food prices data from World Food Programme displayed through VAM Economic Explorer",
                        "image_url": "http://dataviz.vam.wfp.org/_images/home/3_economic.jpg",
                        "url": "http://dataviz.vam.wfp.org/economic_explorer/prices?iso3=BLR",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                    }
                    assert qc_indicators == []
                    (
                        dataset,
                        showcase,
                        qc_indicators,
                    ) = wfp.generate_dataset_and_showcase("PSE")
                    assert dataset == {
                        "name": "wfp-food-prices-for-state-of-palestine",
                        "title": "State of Palestine - Food Prices",
                        "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                        "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                        "data_update_frequency": "7",
                        "groups": [{"name": "pse"}],
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "dataset_source": "FPMA, PCBS, Palestinian Central Bureau of Statistics via FAO: GIEWS, VAM",
                        "dataset_date": "[2007-01-15T00:00:00 TO 2021-07-15T00:00:00]",
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
                        "image_url": "http://dataviz.vam.wfp.org/_images/home/3_economic.jpg",
                        "url": "http://dataviz.vam.wfp.org/economic_explorer/prices?iso3=PSE",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                    }
                    assert qc_indicators == []
                    dataset, showcase = wfp.generate_global_dataset_and_showcase()
                    assert dataset == {
                        "name": "global-wfp-food-prices",
                        "title": "Global - Food Prices",
                        "maintainer": "f1921552-8c3e-47e9-9804-579b14a83ee3",
                        "owner_org": "3ecac442-7fed-448d-8f78-b385ef6f84e7",
                        "data_update_frequency": "7",
                        "groups": [{"name": "world"}],
                        "subnational": "1",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                        ],
                        "dataset_source": "WFP",
                        "dataset_date": "[2007-01-15T00:00:00 TO 2021-07-15T00:00:00]",
                    }
                    assert showcase == {
                        "name": "global-wfp-food-prices-showcase",
                        "title": "Global - Food Prices showcase",
                        "notes": "Global food prices data from World Food Programme displayed through VAM Economic Explorer",
                        "image_url": "http://dataviz.vam.wfp.org/_images/home/3_economic.jpg",
                        "url": "http://dataviz.vam.wfp.org/economic_explorer/prices",
                        "tags": [
                            {
                                "name": "commodities",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "prices",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "markets",
                                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                            },
                            {
                                "name": "hxl",
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
                        "wfp_commodities_global",
                        "wfp_countries_global",
                        "wfp_markets_global",
                    ):
                        csv_filename = f"{filename}.csv"
                        expected_file = join(fixtures_dir, csv_filename)
                        actual_file = join(tempdir, csv_filename)
                        assert_files_same(expected_file, actual_file)
