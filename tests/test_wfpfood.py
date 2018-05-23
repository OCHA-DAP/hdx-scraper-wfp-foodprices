#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Unit tests for wfpfood scraper.

'''
from os.path import join

import pytest
from hdx.hdx_configuration import Configuration
from wfpfood import generate_dataset_and_showcase, get_countriesdata
from hdx.location.country import Country

class TestWfpFood:
    countrydata = {}

    @pytest.fixture(scope='function')
    def configuration(self):
        Configuration._create(hdx_read_only=True,
                              project_config_yaml=join('tests', 'config', 'project_configuration.yml'))
        Locations.set_validlocations([{'name': 'afg', 'title': 'Afghanistan'}])  # add locations used in tests

    @pytest.fixture(scope='function')
    def downloader(self):
        class Response:
            @staticmethod
            def json():
                pass

        class Download:
            @staticmethod
            def download(url):
                response = Response()
                if url == 'http://xxx':
                    def fn():
                        return {'key': [TestWfpFood.countrydata]}
                    response.json = fn
                return response
        return Download()

    #def test_country_load(self):

    def test_country_conversion(self):
        assert Country.get_iso3_country_code("Afghanistan") == "AFG"
        

#    def test_get_countriesdata(self, downloader):
#        countriesdata = get_countriesdata('http://xxx/', downloader)
#        assert countriesdata == [TestWfpFood.countrydata]

#    def test_generate_dataset_and_showcase(self, configuration, downloader):
#        dataset, showcase = generate_dataset_and_showcase(downloader, TestScraperName.countrydata)
#        assert dataset == {}
#
#        resources = dataset.get_resources()
#        assert resources == []
#
#        assert showcase == {}

