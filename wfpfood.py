#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP food prices:
------------

Creates datasets with flattened tables of WFP food prices.

"""

import logging

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {'date': '#date', 'adm1name': '#adm1+name', 'adm2name': '#adm2+name', 'latitude': '#geo+lat',
           'longitude': '#geo+lon', 'market': '#name+market', 'category': '#item+type', 'commodity': '#item+name',
           'unit': '#item+unit', 'currency': '#currency', 'pricetype': '#item+price+type', 'price': '#value'}
headers = list(hxltags.keys())


class WFPFood:
    def __init__(self, configuration, retriever, headers):
        self.configuration = configuration
        self.retriever = retriever
        self.headers = headers
        self.commodity_to_category = dict()

    def get_countries(self):
        url = self.configuration['countries_url']
        json = self.retriever.retrieve_json(url, 'countries.json', 'countries', headers=self.headers)
        countries = set()
        for country in json['response']:
            countries.add((country['iso3'], country['adm0_name']))
        return [{'iso3': x[0], 'name': x[1]} for x in sorted(countries)]

    def get_list(self, endpoint, countryiso3=None, startdate=None):
        url = f'{self.configuration["base_url"]}{endpoint}'
        base_filename = url.split('/')[-2]
        page = 1
        all_data = []
        data = None
        while data is None or len(data) > 0:
            parameters = {'page': page}
            if countryiso3 is None:
                filename = f'{base_filename}_{page}.json'
                log = f'{base_filename} page {page}'
            else:
                filename = f'{base_filename}_{countryiso3}_{page}.json'
                log = f'{base_filename} for {countryiso3} page {page}'
                parameters['CountryCode'] = countryiso3
            if startdate:
                parameters['startDate'] = startdate
            try:
                json = self.retriever.retrieve_json(url, filename, log, False, parameters=parameters,
                                                    headers=self.headers)
            except FileNotFoundError:
                json = {'items': list()}
            data = json['items']
            all_data.extend(data)
            page = page + 1
        return all_data

    def build_mappings(self):
        categoryid_to_name = dict()
        for category in self.get_list('Commodities/Categories/List'):
            categoryid_to_name[category['id']] = category['name']
        for commodity in self.get_list('Commodities/List'):
            self.commodity_to_category[commodity['id']] = categoryid_to_name[commodity['categoryId']]

    def generate_dataset_and_showcase(self, countryiso3, folder):
        countryname = Country.get_country_name_from_iso3(countryiso3)
        title = f'{countryname} - Food Prices'
        logger.info(f'Creating dataset: {title}')
        name = f'WFP food prices for {countryname}'
        slugified_name = slugify(name).lower()

        dataset = Dataset({
            'name': slugified_name,
            'title': title,
        })
        dataset.set_maintainer('eda0ee04-7436-47f0-87ab-d1b9edcd3bb9')  # Wael
        dataset.set_organization('3ecac442-7fed-448d-8f78-b385ef6f84e7')

        dataset.set_expected_update_frequency('weekly')
        dataset.add_country_location(countryname)
        dataset.set_subnational(True)
        tags = ['commodities', 'prices', 'markets', 'hxl']
        dataset.add_tags(tags)

        prices_data = self.get_list('MarketPrices/PriceMonthly', countryiso3)
        if not prices_data:
            logger.info(f'{countryiso3} has no prices data!')
            return None, None
        market_to_adm = dict()
        for market in self.get_list('Markets/List', countryiso3):
            market_to_adm[market['marketId']] = market['admin1Name'], market['admin2Name'], market['marketLatitude'],\
                                                market['marketLongitude']

        rows = dict()
        sources = set()
        for price_data in prices_data:
            if price_data['commodityPriceFlag'] not in ('actual', 'aggregate'):
                continue
            date = price_data['commodityPriceDate']
            category = self.commodity_to_category[price_data['commodityID']]
            mkt = price_data['marketName']
            if mkt == 'National Average':
                adm1 = adm2 = lat = lon = ''
            else:
                market_id = price_data['marketID']
                if market_id in market_to_adm:
                    adm1, adm2, lat, lon = market_to_adm[market_id]
                else:
                    adm1 = adm2 = lat = lon = ''
            orig_source = price_data['commodityPriceSourceName'].replace('+', '/').replace(',', '/')
            split_sources = orig_source.split('/')
            for source in split_sources:
                source = source.strip()
                if source[-1] == '.':
                    source = source[:-1]
                source_lower = source.lower()
                if 'mvam' in source_lower and len(source_lower) <= 8:
                    source = 'WFP mVAM'
                elif '?stica' in source:
                    source = source.replace('?stica', 'ística')
                sources.add(source)
            commodity = price_data['commodityName']
            unit = price_data['commodityUnitName']
            price = price_data['commodityPrice']
            currency = price_data['currencyName']
            pricetype = price_data['commodityPriceFlag']
            key = date, adm1, adm2, mkt, category, commodity, unit
            rows[key] = {'date': date, 'adm1name': adm1, 'adm2name': adm2, 'market': mkt, 'latitude': lat,
                         'longitude': lon, 'category': category, 'commodity': commodity, 'unit': unit,
                         'currency': currency, 'pricetype': pricetype, 'price': price}
        if not rows:
            logger.info(f'{countryiso3} has no prices!')
            return None, None
        dataset['dataset_source'] = ', '.join(sorted(sources))
        filename = f'wfp_food_prices_{countryiso3.lower()}.csv'
        resourcedata = {
            'name': title,
            'description': 'Food prices data with HXL tags',
            'format': 'csv'
        }
        rows = [rows[key] for key in sorted(rows)]
        dataset.generate_resource_from_iterator(headers, rows, hxltags, folder, filename, resourcedata, datecol='date')

        showcase = Showcase({
            'name': f'{slugified_name}-showcase',
            'title': f'{title} showcase',
            'notes': f'{countryname} food prices data from World Food Programme displayed through VAM Economic Explorer',
            'url': f'http://dataviz.vam.wfp.org/economic_explorer/prices?iso3={countryiso3}',
            'image_url': 'http://dataviz.vam.wfp.org/_images/home/3_economic.jpg'
        })
        showcase.add_tags(tags)
        return dataset, showcase