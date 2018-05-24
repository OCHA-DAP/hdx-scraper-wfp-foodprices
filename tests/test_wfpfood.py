#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for wfpfood scraper.

'''
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from wfpfood import *
from hdx.location.country import Country

class TestWfpFood:
    countrydata = [
        ["Afghanistan", "1"],
        ["Aksai Chin", "2"],
        ["Albania", "3"]
    ]

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])  # add locations used in tests

    @pytest.fixture(scope='function')
    def downloader(self):
        class Download:
            def get_tabular_rows(self, url, **kwargs):
                if url == 'http://xxx':
                    return TestWfpFood.countrydata
        return Download()

    def test_country_conversion(self):
        assert Country.get_iso3_country_code("Afghanistan") == "AFG"
        

    def test_get_countriesdata(self, downloader):
        countriesdata = get_countriesdata('http://xxx', downloader)
        assert countriesdata == [
            dict(name="Afghanistan", code="1", iso3="AFG"),
            dict(name="Albania",     code="3", iso3="ALB")
        ]

    def test_months_between(self):
        assert [] == list(months_between("2008/05/01", "2008/04/01"))
        assert ["2008-05-01"] == list(months_between("2008/05/01", "2008/05/01"))
        assert ["2008-05-01","2008-06-01","2008-07-01","2008-08-01"] == list(months_between("2008/05/01", "2008/08/01"))

#    def test_generate_dataset_and_showcase(self, configuration, downloader):
#        dataset, showcase = generate_dataset_and_showcase(downloader, TestScraperName.countrydata)
#        assert dataset == {}
#
#        resources = dataset.get_resources()
#        assert resources == []
#
#        assert showcase == {}

