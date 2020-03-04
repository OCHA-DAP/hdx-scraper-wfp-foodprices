#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for wfpfood scraper.

'''
from os.path import join

import pytest
from hdx.data.vocabulary import Vocabulary
from hdx.utilities.path import temp_dir

from wfpfood import *
from hdx.location.country import Country
from hdx.hdx_configuration import Configuration
from hdx.hdx_locations import Locations


class TestWfpFood:
    countrydata = [
        ["Afghanistan", "1"],
        ["Aksai Chin", "2"],
        ["Albania", "3"]
    ]
    countrydata1 = [
        ["Afghanistan", "1"],
        ["Aksai Chin", "2"],
        ["Albania", "3"],
        ["China", "4"]
    ]

    countriesdata = [
        {'name': "Afghanistan", 'code': "1", 'iso3': "AFG", 'wfp_countries': [{'name': "Afghanistan", 'code': "1"}]},
        {'name': "Albania", 'code': "3", 'iso3': "ALB", 'wfp_countries': [{'name': "Albania", 'code': "3"}]},
        {'name': "China", 'code': "2", 'iso3': "CHN", 'wfp_countries': [{'name': "Aksai Chin", 'code': "2"}]},
    ]

    country_correspondence = {
        "Aksai Chin": "China"
    }

    Afghanistan_data = [
        {"currency": "AFN", "startdate": "2014/01/15", "enddate": "2014/04/15", "mktid": 266, "cmid": 55, "ptid": 15,
         "umid": 5, "catid": 1, "unit": "KG", "cmname": "Bread - Retail", "category": "cereals and tubers",
         "mktname": "Fayzabad", "admname": "Badakhshan", "adm1id": 272, "sn": "266_55_15_5",
         "mp_price": [50, 51, None, 52]},
        {"currency": "AFN", "startdate": "2014/02/15", "enddate": "2018/04/15", "mktid": 266, "cmid": 305, "ptid": 15,
         "umid": 56, "catid": 8, "unit": "USD/LCU", "cmname": "Exchange rate - Retail", "category": "non-food",
         "mktname": "Fayzabad", "admname": "Badakhshan", "adm1id": 272, "sn": "266_305_15_56",
         "mp_price": [57.55, None, None, None, None, 56.6, 56.625, 57, 57.375, 57.65, 57.725, 57.55, 57.225, 57.475,
                      57.625, 59.225, 60.1, 60.35, 63.45, 63.6, 64.175, 66.125, 67.475, 68.325, None, 68.275, 68.175,
                      68.375, 68.65, 68.35, 66.85, 67.4, 65.65, 66.4, 66.55, 66.575, 66.95, 67.625, 67.175, 67.55,
                      67.85, 68.2125, 68.25, 68.2875, 67.9875, 68.35, 68.925, 69.475, 68.475, 69, 69.525]}]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True, user_agent='test',
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])  # add locations used in tests
        Country.countriesdata(use_live=False)
        Vocabulary._tags_dict = True
        Vocabulary._approved_vocabulary = {
            'tags': [{'name': 'hxl'}, {'name': 'commodities'}, {'name': 'prices'}, {'name': 'markets'}],
            'id': '4e61d464-4943-4e97-973a-84673c1aaa87', 'name': 'approved'}

    @pytest.fixture(scope='function')
    def downloader(self):
        class Download:
            def get_tabular_rows(self, url, **kwargs):
                if url == 'config/adm0code.csv':
                    return None, TestWfpFood.countrydata
                if url == 'http://yyy?ac=1':
                    return None, TestWfpFood.Afghanistan_data
                return []

        return Download()

    def test_country_conversion(self):
        assert Country.get_iso3_country_code("Afghanistan") == "AFG"
        assert Country.get_iso3_country_code("China") == "CHN"

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('config/adm0code.csv', downloader, self.country_correspondence)
        assert countriesdata == TestWfpFood.countriesdata

    def test_get_countriesdata1(self):
        class Download1:
            def get_tabular_rows(self, url, **kwargs):
                if url == 'config/adm0code.csv':
                    return None, TestWfpFood.countrydata1
                if url == 'http://yyy?ac=1':
                    return TestWfpFood.Afghanistan_data
                return []

        countriesdata = get_countriesdata('config/adm0code.csv', Download1(), self.country_correspondence)
        assert countriesdata == [
            dict(name="Afghanistan", code="1", iso3="AFG", wfp_countries=[dict(name="Afghanistan", code="1")]),
            dict(name="Albania", code="3", iso3="ALB", wfp_countries=[dict(name="Albania", code="3")]),
            dict(name="China", code="4", iso3="CHN", wfp_countries=[dict(name="Aksai Chin", code="2"),
                                                                    dict(name="China", code="4")]),
        ]

    def test_months_between(self):
        assert [] == list(months_between("2008/05/01", "2008/04/01"))
        assert ["2008-05-01"] == list(months_between("2008/05/01", "2008/05/01"))
        assert ["2008-05-01", "2008-06-01", "2008-07-01", "2008-08-01"] == list(
            months_between("2008/05/01", "2008/08/01"))

    def test_read_flattened_data(self, downloader):
        countrydata = TestWfpFood.countriesdata[0]
        data = read_flattened_data('http://yyy?ac=', downloader, countrydata)
        data = list(data)
        assert len(data) == 49
        assert data[0]["country"] == "Afghanistan"
        assert data[0]["price"] == 50
        assert data[1]["price"] == 51
        assert data[2]["price"] == 52
        assert data[0]["date"] == "2014-01-15"
        assert data[1]["date"] == "2014-02-15"
        assert data[2]["date"] == "2014-04-15"

    def test_dataframe(self, downloader):
        countrydata = TestWfpFood.countriesdata[0]
        df = flattened_data_to_dataframe(read_flattened_data('http://yyy?ac=', downloader, countrydata))
        assert len(df) == 50
        assert df.iloc[0].date == "#date"
        assert df.iloc[0].price.startswith("#value")
        assert df.iloc[1].price == 50
        assert df.iloc[2].price == 51
        assert df.iloc[3].price == 52
        assert df.iloc[1].date == "2014-01-15"
        assert df.iloc[2].date == "2014-02-15"
        assert df.iloc[3].date == "2014-04-15"

    def test_generate_dataset_and_showcase(self, downloader, configuration):
        countrydata = TestWfpFood.countriesdata[0]
        with temp_dir('wfp-foodprices') as folder:
            dataset, showcase = generate_dataset_and_showcase('http://yyy?ac=', downloader, folder, countrydata, {})

            assert dataset["name"] == "wfp-food-prices-for-afghanistan"
            assert dataset["title"] == "Afghanistan - Food Prices"

            resources = dataset.get_resources()
            assert resources[0] == {'format': 'csv', 'description': 'Food prices data with HXL tags',
                                    'name': 'Afghanistan - Food Prices', 'dataset_preview_enabled': 'False',
                                    'resource_type': 'file.upload', 'url_type': 'upload'}
            assert resources[1]["name"] == 'Afghanistan - Food Median Prices'

            assert showcase["title"] == "Afghanistan - Food Prices showcase"
            assert showcase["url"] == "http://dataviz.vam.wfp.org/economic_explorer/prices?adm0=1"
