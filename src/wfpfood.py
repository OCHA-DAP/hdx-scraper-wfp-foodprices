#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
WFP food prices:
------------

Creates datasets with flattened tables of WFP food prices.

"""
import difflib
import logging
import re

from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.location.currency import Currency, CurrencyError
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from slugify import slugify

from database.dbcommodity import DBCommodity
from database.dbfoodprice import DBFoodPrice
from database.dbmarket import DBMarket

logger = logging.getLogger(__name__)

hxltags = {'date': '#date', 'countryiso3': '#country+code', 'adm1name': '#adm1+name', 'adm2name': '#adm2+name',
           'market_id': '#loc+market+code', 'market': '#loc+market+name', 'latitude': '#geo+lat',
           'longitude': '#geo+lon', 'category': '#item+type', 'commodity_id': '#item+code', 'commodity': '#item+name',
           'unit': '#item+unit', 'pricetype': '#item+price+type', 'currency': '#currency', 'price': '#value',
           'usdprice': '#value+usd'}
country_headers = ['date', 'adm1name', 'adm2name', 'market', 'latitude', 'longitude', 'category', 'commodity', 'unit',
                   'pricetype', 'currency', 'price', 'usdprice']
qc_hxltags = {'date': '#date', 'code': '#meta+code', 'usdprice': '#value+usd'}


class WFPFood:
    def __init__(self, configuration, token_downloader, retriever, session):
        self.configuration = configuration
        self.token_downloader = token_downloader
        self.retriever = retriever
        self.session = session
        self.headers = None
        self.commodity_to_category = dict()
        Currency.setup(retriever=retriever, fallback_historic_to_current=True, fallback_current_to_static=False)

    def refresh_headers(self):
        self.token_downloader.download(self.configuration['token_url'], post=True,
                                       parameters={'grant_type': 'client_credentials'})
        access_token = self.token_downloader.get_json()['access_token']
        self.headers = {'Accept': 'application/json', 'Authorization': f'Bearer {access_token}'}

    def retrieve(self, url, filename, log, parameters=None):
        try:
            results = self.retriever.retrieve_json(url, filename, log, False, parameters=parameters,
                                                   headers=self.headers)
        except DownloadError:
            if self.retriever.downloader.response.status_code != 401:
                raise
            self.refresh_headers()
            results = self.retriever.retrieve_json(url, filename, log, False, parameters=parameters,
                                                   headers=self.headers)
        return results

    def get_countries(self):
        url = self.configuration['countries_url']
        json = self.retrieve(url, 'countries.json', 'countries')
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
                json = self.retrieve(url, filename, log, parameters)
            except FileNotFoundError:
                json = {'items': list()}
            data = json['items']
            all_data.extend(data)
            page = page + 1
        return all_data

    def build_mappings(self):
        self.session.query(DBCommodity).delete()
        categoryid_to_name = dict()
        for category in self.get_list('Commodities/Categories/List'):
            categoryid_to_name[category['id']] = category['name']
        for commodity in self.get_list('Commodities/List'):
            commodity_id = commodity['id']
            commodity_name = commodity['name']
            category = categoryid_to_name[commodity['categoryId']]
            self.commodity_to_category[commodity_id] = categoryid_to_name[commodity['categoryId']]
            dbcommodity = DBCommodity(id=commodity_id, category=category, name=commodity_name)
            self.session.add(dbcommodity)
        self.session.commit()

    @staticmethod
    def match_source(sources, source):
        words = source.split(' ')
        if len(words) < 2:
            return False
        found = False
        for cursource in sources:
            words = cursource.split(' ')
            if len(words) < 2:
                continue
            seq = difflib.SequenceMatcher(None, source, cursource)
            if seq.ratio() > 0.9:
                found = True
        return found

    def get_dataset_and_showcase(self, countryiso3):
        if countryiso3 == 'global':
            location = 'world'
            countryname = 'Global'
            name = 'Global WFP food prices'
        else:
            location = countryiso3
            countryname = Country.get_country_name_from_iso3(countryiso3)
            name = f'WFP food prices for {countryname}'
        title = f'{countryname} - Food Prices'
        logger.info(f'Creating dataset: {title}')
        slugified_name = slugify(name).lower()

        dataset = Dataset({
            'name': slugified_name,
            'title': title,
        })
        dataset.set_maintainer('f1921552-8c3e-47e9-9804-579b14a83ee3')
        dataset.set_organization('3ecac442-7fed-448d-8f78-b385ef6f84e7')

        dataset.set_expected_update_frequency('weekly')
        try:
            dataset.add_country_location(location)
        except HDXError:
            try:
                dataset.add_other_location(location)
            except HDXError as e:
                logger.exception('%s has a problem! %s' % (countryname, e))
                return None, None
        dataset.set_subnational(True)
        tags = ['commodities', 'prices', 'markets', 'hxl']
        dataset.add_tags(tags)
        showcase = Showcase({
            'name': f'{slugified_name}-showcase',
            'title': f'{title} showcase',
            'notes': f'{countryname} food prices data from World Food Programme displayed through VAM Economic Explorer',
            'image_url': 'http://dataviz.vam.wfp.org/_images/home/3_economic.jpg'
        })
        if countryiso3 == 'global':
            showcase['url'] = f'http://dataviz.vam.wfp.org/economic_explorer/prices'
        else:
            showcase['url'] = f'http://dataviz.vam.wfp.org/economic_explorer/prices?iso3={countryiso3}'
        showcase.add_tags(tags)

        return dataset, showcase

    def generate_dataset_and_showcase(self, countryiso3, folder):
        dataset, showcase = self.get_dataset_and_showcase(countryiso3)
        if not dataset:
            return None, None, None
        prices_data = self.get_list('MarketPrices/PriceMonthly', countryiso3)
        if not prices_data:
            logger.info(f'{countryiso3} has no prices data!')
            return None, None, None
        market_to_adm = dict()
        self.session.query(DBMarket).filter(DBMarket.countryiso3 == countryiso3).delete()
        for market in self.get_list('Markets/List', countryiso3):
            market_id = market['marketId']
            market_name = market['marketName']
            admin1 = market['admin1Name']
            admin2 = market['admin2Name']
            latitude = market['marketLatitude']
            longitude = market['marketLongitude']
            market_to_adm[market_id] = admin1, admin2, latitude, longitude
            dbmarket = DBMarket(id=market_id, name=market_name, countryiso3=countryiso3, admin1=admin1, admin2=admin2,
                                latitude=latitude, longitude=longitude)
            self.session.add(dbmarket)
        self.session.commit()
        logger.info(f'{len(prices_data)} prices rows')
        rows = dict()
        sources = dict()
        markets = dict()
        for price_data in prices_data:
            pricetype = price_data['commodityPriceFlag']
            if pricetype not in ('actual', 'aggregate'):
                continue
            commodity_id = price_data['commodityID']
            category = self.commodity_to_category[commodity_id]
            market_id = price_data['marketID']
            market = price_data['marketName']
            result = market_to_adm.get(market_id)
            if result:
                adm1, adm2, lat, lon = result
            else:
                adm1 = adm2 = lat = lon = ''
                market_to_adm[market_id] = adm1, adm2, lat, lon
                dbmarket = DBMarket(id=market_id, name=market, countryiso3=countryiso3)
                self.session.add(dbmarket)
            if market == 'National Average':
                adm1 = adm2 = lat = lon = ''
            else:
                if market_id in market_to_adm:
                    adm1, adm2, lat, lon = market_to_adm[market_id]
                else:
                    adm1 = adm2 = lat = lon = ''
            orig_source = price_data['commodityPriceSourceName'].replace('M/o', 'Ministry of').replace('+', '/')
            regex = r'Government.*,(Ministry.*)'
            match = re.search(regex, orig_source)
            if match:
                split_sources = [match.group(1)]
            else:
                split_sources = orig_source.replace(',', '/').replace(';', '/').split('/')
            for source in split_sources:
                source = source.strip()
                if not source:
                    continue
                if source[-1] == '.':
                    source = source[:-1]
                source_lower = source.lower()
                if 'mvam' in source_lower and len(source_lower) <= 8:
                    source = 'WFP mVAM'
                elif '?stica' in source:
                    source = source.replace('?stica', 'ística')
                source_lower = source.lower()
                if not self.match_source(sources.keys(), source_lower):
                    sources[source_lower] = source
            date_str = price_data['commodityPriceDate']
            date = parse_date(date_str)
            date_str = date.date().isoformat()
            commodity = price_data['commodityName']
            unit = price_data['commodityUnitName']
            price = round(price_data['commodityPrice'], 2)
            currency = price_data['currencyName']
            try:
                usdprice = Currency.get_historic_value_in_usd(price, currency, date)
                usdprice = round(usdprice, 2)
            except CurrencyError:
                usdprice = None
            key = date, adm1, adm2, market, category, commodity, unit
            if key not in rows:
                rows[key] = {'date': date_str, 'adm1name': adm1, 'adm2name': adm2, 'market': market, 'latitude': lat,
                             'longitude': lon, 'category': category, 'commodity': commodity, 'unit': unit,
                             'pricetype': pricetype, 'currency': currency, 'price': price, 'usdprice': usdprice}
                if pricetype == 'actual':
                    pricetype_b = False
                else:
                    pricetype_b = True
                dbfoodprice = DBFoodPrice(date=date, countryiso3=countryiso3, market_id=market_id,
                                          commodity_id=commodity_id, unit=unit, pricetype=pricetype_b,
                                          currency=currency, price=price, usdprice=usdprice)
                self.session.add(dbfoodprice)
            if adm1 and adm2 and category and usdprice:
                adm1adm2market = adm1, adm2, market
                commodities = markets.get(adm1adm2market, dict())
                dict_of_lists_add(commodities, (commodity, unit, currency), (date_str, usdprice))
                markets[adm1adm2market] = commodities
        if not rows:
            logger.info(f'{countryiso3} has no prices!')
            return None, None, None
        logger.info(f'{len(rows)} unique prices rows of price type actual or aggregate')
        number_market = list()
        for key, commodities in markets.items():
            number_market.append((len(commodities), key))
        number_market = sorted(number_market, reverse=True)
        qc_indicators = list()
        qc_rows = [qc_hxltags]
        chosen_commodities = set()
        # Go through markets starting with the one with most commodities
        for _, adm1adm2market in number_market:
            commodities = markets[adm1adm2market]
            number_commodity = list()
            for commodityunitcurrency, details in commodities.items():
                number_commodity.append((len(details), commodityunitcurrency))
            number_commodity = sorted(number_commodity, reverse=True)
            index = 0
            # Pick commodity with most rows that has not already been used for another market
            commodity, unit, currency = number_commodity[index][1]
            while commodity in chosen_commodities:
                index += 1
                if index == len(number_commodity):
                    commodity, unit, currency = number_commodity[0][1]
                    break
                commodity, unit, currency = number_commodity[index][1]
            adm1, adm2, market = adm1adm2market
            code = f'{adm1}-{adm2}-{market}-{commodity}-{unit}-{currency}'
            for date, usdprice in sorted(commodities[(commodity, unit, currency)]):
                qc_rows.append({'date': date, 'code': code, 'usdprice': usdprice})
            chosen_commodities.add(commodity)
            marketname = market
            if adm2 != market:
                marketname = f'{adm2}/{marketname}'
            if adm1 != adm2:
                marketname = f'{adm1}/{marketname}'
            qc_indicators.append({'code': code, 'title': f'Price of {commodity} in {market}',
                                  'unit': 'US Dollars ($)',
                                  'description': f'Price of {commodity} ($/{unit}) in {marketname}',
                                  'code_col': '#meta+code', 'value_col': '#value+usd', 'date_col': '#date'})
            if len(qc_indicators) == 3:
                break
        dataset['dataset_source'] = ', '.join(sorted(sources.values()))
        filename = f'wfp_food_prices_{countryiso3.lower()}.csv'
        resourcedata = {
            'name': dataset['title'],
            'description': 'Food prices data with HXL tags',
            'format': 'csv'
        }
        rows = [rows[key] for key in sorted(rows)]
        country_hxltags = {header: hxltags[header] for header in country_headers}
        dataset.generate_resource_from_iterator(country_headers, rows, country_hxltags, folder, filename, resourcedata, datecol='date')
        filename = f'wfp_food_prices_{countryiso3.lower()}_qc.csv'
        resourcedata = {
            'name': f'QuickCharts: {dataset["title"]}',
            'description': 'Food prices QuickCharts data with HXL tags',
            'format': 'csv'
        }
        dataset.generate_resource_from_rows(folder, filename, qc_rows, resourcedata, headers=list(qc_hxltags.keys()))
        return dataset, showcase, qc_indicators

    def remove_rows(self, countryiso3):
        self.session.query(DBFoodPrice).filter(DBFoodPrice.countryiso3 == countryiso3).delete()

    def update_database(self):
        self.session.commit()

    def generate_global_dataset_and_showcase(self, folder):
        dataset, showcase = self.get_dataset_and_showcase('global')
        dataset['dataset_source'] = 'WFP'

        def dbtable_to_list(cls, fn, rsdata, datecol=None):
            rows = list()
            for result in self.session.query(cls):
                row = dict()
                for column in result.__table__.columns.keys():
                    row[column] = getattr(result, column)
                    if column == 'date':
                        row[column] = row[column].date().isoformat()
                    elif column == 'pricetype':
                        if row[column]:
                            row[column] = 'ag'
                        else:
                            row[column] = 'ac'
                    rows.append(row)
            hdrs = cls.__table__.columns.keys()
            htgs = {header: hxltags[header] for header in hdrs}
            dataset.generate_resource_from_iterator(hdrs, rows, htgs, folder, fn, rsdata, datecol=datecol)

        filename = f'wfp_food_prices_global.csv'
        resourcedata = {
            'name': dataset['title'],
            'description': 'Food prices data with HXL tags',
            'format': 'csv'
        }
        dbtable_to_list(DBFoodPrice, filename, resourcedata, datecol='date')
        filename = f'wfp_commodities_global.csv'
        resourcedata = {
            'name': 'Global WFP commodities',
            'description': 'Commodities data with HXL tags',
            'format': 'csv'
        }
        dbtable_to_list(DBCommodity, filename, resourcedata)
        filename = f'wfp_markets_global.csv'
        resourcedata = {
            'name': 'Global WFP markets',
            'description': 'Markets data with HXL tags',
            'format': 'csv'
        }
        dbtable_to_list(DBMarket, filename, resourcedata)
        return dataset, showcase


