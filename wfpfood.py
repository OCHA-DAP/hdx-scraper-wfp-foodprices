#!/usr/bin/python
"""
WFP food prices:
------------

Creates datasets with flattened tables of WFP food prices.

"""
import difflib
import logging
import re
from os.path import join

from database.dbcommodity import DBCommodity
from database.dbcountry import DBCountry
from database.dbmarket import DBMarket
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.location.currency import Currency, CurrencyError
from hdx.utilities.dateparse import default_date, default_enddate, now_utc, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import DownloadError
from hdx.utilities.loader import load_text
from hdx.utilities.saver import save_text
from slugify import slugify

logger = logging.getLogger(__name__)

hxltags = {
    "date": "#date",
    "countryiso3": "#country+code",
    "admin1": "#adm1+name",
    "admin2": "#adm2+name",
    "market_id": "#loc+market+code",
    "market": "#loc+market+name",
    "latitude": "#geo+lat",
    "longitude": "#geo+lon",
    "category": "#item+type",
    "commodity_id": "#item+code",
    "commodity": "#item+name",
    "unit": "#item+unit",
    "priceflag": "#item+price+flag",
    "pricetype": "#item+price+type",
    "currency": "#currency",
    "price": "#value",
    "usdprice": "#value+usd",
    "url": "#country+url",
    "start_date": "#date+start",
    "end_date": "#date+end",
}
country_headers = [
    "date",
    "admin1",
    "admin2",
    "market",
    "latitude",
    "longitude",
    "category",
    "commodity",
    "unit",
    "priceflag",
    "pricetype",
    "currency",
    "price",
    "usdprice",
]
qc_hxltags = {"date": "#date", "code": "#meta+code", "usdprice": "#value+usd"}


class WFPFood:
    def __init__(self, configuration, folder, token_downloader, retriever, session):
        self.configuration = configuration
        self.folder = folder
        self.token_downloader = token_downloader
        self.retriever = retriever
        self.session = session
        self.headers = None
        self.commodity_to_category = dict()
        if retriever.save:
            fixed_now = now_utc()
            datestring = fixed_now.isoformat()
            path = join(retriever.saved_dir, "now.txt")
            save_text(datestring, path)
        elif retriever.use_saved:
            path = join(retriever.saved_dir, "now.txt")
            datestring = load_text(path)
            fixed_now = parse_date(datestring, include_microseconds=True)
        else:
            fixed_now = None
        Currency.setup(
            retriever=retriever,
            fallback_historic_to_current=True,
            fallback_current_to_static=False,
            fixed_now=fixed_now,
        )

    def refresh_headers(self):
        self.token_downloader.download(
            self.configuration["token_url"],
            post=True,
            parameters={"grant_type": "client_credentials"},
        )
        access_token = self.token_downloader.get_json()["access_token"]
        self.headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        }

    def retrieve(self, url, filename, log, parameters=None):
        try:
            results = self.retriever.download_json(
                url, filename, log, False, parameters=parameters, headers=self.headers
            )
        except DownloadError:
            if self.retriever.downloader.response.status_code not in (104, 401, 403):
                raise
            self.refresh_headers()
            results = self.retriever.download_json(
                url, filename, log, False, parameters=parameters, headers=self.headers
            )
        return results

    def get_countries(self):
        url = self.configuration["countries_url"]
        json = self.retrieve(url, "countries.json", "countries")
        countries = set()
        for country in json["response"]:
            if self.retriever.save and country["iso3"] not in ("BLR", "COG", "PSE"):
                continue
            countries.add((country["iso3"], country["adm0_name"]))
        return [{"iso3": x[0], "name": x[1]} for x in sorted(countries)]

    def get_list(self, endpoint, countryiso3=None, startdate=None):
        all_data = list()
        url = f'{self.configuration["base_url"]}{endpoint}'
        base_filename = url.split("/")[-2]
        if countryiso3 == "PSE":  # hack as PSE is treated by WFP as 2 areas
            countryiso3s = ["PSW", "PSG"]
        else:
            countryiso3s = [countryiso3]
        for countryiso3 in countryiso3s:
            page = 1
            data = None
            while data is None or len(data) > 0:
                parameters = {"page": page}
                if countryiso3 is None:
                    filename = f"{base_filename}_{page}.json"
                    log = f"{base_filename} page {page}"
                else:
                    filename = f"{base_filename}_{countryiso3}_{page}.json"
                    log = f"{base_filename} for {countryiso3} page {page}"
                    parameters["CountryCode"] = countryiso3
                if startdate:
                    parameters["startDate"] = startdate
                try:
                    json = self.retrieve(url, filename, log, parameters)
                except FileNotFoundError:
                    json = {"items": list()}
                data = json["items"]
                all_data.extend(data)
                page = page + 1
        return all_data

    def build_mappings(self):
        self.session.query(DBCommodity).delete()
        categoryid_to_name = dict()
        for category in self.get_list("Commodities/Categories/List"):
            categoryid_to_name[category["id"]] = category["name"]
        for commodity in self.get_list("Commodities/List"):
            commodity_id = commodity["id"]
            commodity_name = commodity["name"]
            category = categoryid_to_name[commodity["categoryId"]]
            self.commodity_to_category[commodity_id] = categoryid_to_name[
                commodity["categoryId"]
            ]
            dbcommodity = DBCommodity(
                commodity_id=commodity_id, category=category, commodity=commodity_name
            )
            self.session.add(dbcommodity)
        self.session.commit()

    @staticmethod
    def match_source(sources, source):
        words = source.split(" ")
        if len(words) < 2:
            return False
        found = False
        for cursource in sources:
            words = cursource.split(" ")
            if len(words) < 2:
                continue
            seq = difflib.SequenceMatcher(None, source, cursource)
            if seq.ratio() > 0.9:
                found = True
        return found

    def get_dataset_and_showcase(self, countryiso3):
        if countryiso3 == "global":
            location = "world"
            countryname = "Global"
            name = "Global WFP food prices"
        else:
            location = countryiso3
            countryname = Country.get_country_name_from_iso3(countryiso3)
            name = f"WFP food prices for {countryname}"
        title = f"{countryname} - Food Prices"
        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(name).lower()

        dataset = Dataset(
            {
                "name": slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("f1921552-8c3e-47e9-9804-579b14a83ee3")
        dataset.set_organization("3ecac442-7fed-448d-8f78-b385ef6f84e7")

        dataset.set_expected_update_frequency("As Needed")
        try:
            dataset.add_country_location(location)
        except HDXError:
            try:
                dataset.add_other_location(location)
            except HDXError as e:
                logger.exception(f"{countryname} has a problem! {e}")
                return None, None
        dataset.set_subnational(True)
        tags = ("hxl", "economics", "food security", "indicators", "markets")
        dataset.add_tags(tags)
        showcase = Showcase(
            {
                "name": f"{slugified_name}-showcase",
                "title": f"{title} showcase",
                "notes": f"{countryname} food prices data from World Food Programme displayed through VAM Economic Explorer",
                "image_url": "http://dataviz.vam.wfp.org/_images/home/3_economic.jpg",
            }
        )
        if countryiso3 == "global":
            showcase["url"] = "http://dataviz.vam.wfp.org/economic_explorer/prices"
        else:
            showcase[
                "url"
            ] = f"http://dataviz.vam.wfp.org/economic_explorer/prices?iso3={countryiso3}"
        showcase.add_tags(tags)

        return dataset, showcase

    def generate_dataset_and_showcase(self, countryiso3):
        dataset, showcase = self.get_dataset_and_showcase(countryiso3)
        if not dataset:
            return None, None, None
        prices_data = self.get_list("MarketPrices/PriceMonthly", countryiso3)
        if not prices_data:
            logger.info(f"{countryiso3} has no prices data!")
            return None, None, None
        market_to_adm = dict()
        dbmarkets = list()
        for market in self.get_list("Markets/List", countryiso3):
            market_id = market["marketId"]
            market_name = market["marketName"]
            admin1 = market["admin1Name"]
            admin2 = market["admin2Name"]
            latitude = market["marketLatitude"]
            longitude = market["marketLongitude"]
            market_to_adm[market_id] = admin1, admin2, latitude, longitude
            dbmarkets.append(
                DBMarket(
                    market_id=market_id,
                    market=market_name,
                    countryiso3=countryiso3,
                    admin1=admin1,
                    admin2=admin2,
                    latitude=latitude,
                    longitude=longitude,
                )
            )
        logger.info(f"{len(prices_data)} prices rows")
        rows = dict()
        sources = dict()
        markets = dict()
        for price_data in prices_data:
            commodity_id = price_data["commodityID"]
            category = self.commodity_to_category[commodity_id]
            market_id = price_data["marketID"]
            market_name = price_data["marketName"]
            if market_name == "National Average":
                adm1 = adm2 = lat = lon = ""
            else:
                result = market_to_adm.get(market_id)
                if result:
                    adm1, adm2, lat, lon = result
                else:
                    adm1 = adm2 = lat = lon = ""
                    market_to_adm[market_id] = adm1, adm2, lat, lon
                    dbmarkets.append(
                        DBMarket(
                            market_id=market_id,
                            market=market_name,
                            countryiso3=countryiso3,
                        )
                    )

            orig_source = (
                price_data["commodityPriceSourceName"]
                .replace("M/o", "Ministry of")
                .replace("+", "/")
            )
            regex = r"Government.*,(Ministry.*)"
            match = re.search(regex, orig_source)
            if match:
                split_sources = [match.group(1)]
            else:
                split_sources = (
                    orig_source.replace(",", "/").replace(";", "/").split("/")
                )
            for source in split_sources:
                source = source.strip()
                if not source:
                    continue
                if source[-1] == ".":
                    source = source[:-1]
                source_lower = source.lower()
                if "mvam" in source_lower and len(source_lower) <= 8:
                    source = "WFP mVAM"
                elif "?stica" in source:
                    source = source.replace("?stica", "ística")
                source_lower = source.lower()
                if not self.match_source(sources.keys(), source_lower):
                    sources[source_lower] = source
            date_str = price_data["commodityPriceDate"]
            date = parse_date(date_str)
            date_str = date.date().isoformat()
            commodity = price_data["commodityName"]
            unit = price_data["commodityUnitName"]
            priceflag = price_data["commodityPriceFlag"]
            pricetype = price_data["priceTypeName"]
            price = price_data["commodityPrice"]
            currency = price_data["currencyName"]
            currency = self.configuration["currency_mappings"].get(currency, currency)
            try:
                usdprice = Currency.get_historic_value_in_usd(price, currency, date)
                usdprice = round(usdprice, 4)
            except CurrencyError:
                usdprice = None
            price = round(price, 2)
            key = (
                priceflag,
                date,
                adm1,
                adm2,
                market_name,
                category,
                commodity,
                unit,
                pricetype,
            )
            if key not in rows:
                rows[key] = {
                    "date": date_str,
                    "admin1": adm1,
                    "admin2": adm2,
                    "market": market_name,
                    "latitude": lat,
                    "longitude": lon,
                    "category": category,
                    "commodity": commodity,
                    "unit": unit,
                    "priceflag": priceflag,
                    "pricetype": pricetype,
                    "currency": currency,
                    "price": price,
                    "usdprice": usdprice,
                }
            if adm1 and adm2 and category and usdprice:
                adm1adm2market = adm1, adm2, market_name
                commodities = markets.get(adm1adm2market, dict())
                dict_of_lists_add(
                    commodities, (commodity, unit, currency), (date_str, usdprice)
                )
                markets[adm1adm2market] = commodities
        if not rows:
            logger.info(f"{countryiso3} has no prices!")
            return None, None, None
        logger.info(f"{len(rows)} unique prices rows of price type actual or aggregate")
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
            adm1, adm2, market_name = adm1adm2market
            code = f"{adm1}-{adm2}-{market_name}-{commodity}-{unit}-{currency}"
            for date, usdprice in sorted(commodities[(commodity, unit, currency)]):
                qc_rows.append({"date": date, "code": code, "usdprice": usdprice})
            chosen_commodities.add(commodity)
            marketname = market_name
            if adm2 != market_name:
                marketname = f"{adm2}/{marketname}"
            if adm1 != adm2:
                marketname = f"{adm1}/{marketname}"
            qc_indicators.append(
                {
                    "code": code,
                    "title": f"Price of {commodity} in {market_name}",
                    "unit": "US Dollars ($)",
                    "description": f"Price of {commodity} ($/{unit}) in {marketname}",
                    "code_col": "#meta+code",
                    "value_col": "#value+usd",
                    "date_col": "#date",
                }
            )
            if len(qc_indicators) == 3:
                break
        dataset["dataset_source"] = ", ".join(sorted(sources.values()))
        filename = f"wfp_food_prices_{countryiso3.lower()}.csv"
        resourcedata = {
            "name": dataset["title"],
            "description": "Food prices data with HXL tags",
            "format": "csv",
        }
        rows = [rows[key] for key in sorted(rows)]
        country_hxltags = {header: hxltags[header] for header in country_headers}
        dataset.generate_resource_from_iterator(
            country_headers,
            rows,
            country_hxltags,
            self.folder,
            filename,
            resourcedata,
            datecol="date",
        )
        filename = f"wfp_food_prices_{countryiso3.lower()}_qc.csv"
        resourcedata = {
            "name": f'QuickCharts: {dataset["title"]}',
            "description": "Food prices QuickCharts data with HXL tags",
            "format": "csv",
        }
        dataset.generate_resource_from_rows(
            self.folder,
            filename,
            qc_rows,
            resourcedata,
            headers=list(qc_hxltags.keys()),
        )
        dataset_date = dataset.get_reference_period()
        self.session.query(DBCountry).filter(
            DBCountry.countryiso3 == countryiso3
        ).delete()
        self.session.query(DBMarket).filter(
            DBMarket.countryiso3 == countryiso3
        ).delete()
        dbcountry = DBCountry(
            countryiso3=countryiso3,
            start_date=dataset_date["startdate"],
            end_date=dataset_date["enddate"],
            url=dataset.get_hdx_url(),
        )
        self.session.add(dbcountry)
        for dbmarket in dbmarkets:
            self.session.add(dbmarket)
        self.session.commit()

        return dataset, showcase, qc_indicators

    def update_database(self):
        self.session.commit()

    def generate_global_dataset_and_showcase(self):
        dataset, showcase = self.get_dataset_and_showcase("global")
        dataset["dataset_source"] = "WFP"

        start_date = default_enddate
        end_date = default_date

        def dbtable_to_list(cls, fn, rsdata):
            nonlocal start_date, end_date
            rows = list()
            for result in self.session.query(cls):
                row = dict()
                for column in result.__table__.columns.keys():
                    row[column] = getattr(result, column)
                    if column == "start_date":
                        if row[column] < start_date:
                            start_date = row[column]
                    elif column == "end_date":
                        if row[column] > end_date:
                            end_date = row[column]
                rows.append(row)
            hdrs = cls.__table__.columns.keys()
            htgs = {header: hxltags[header] for header in hdrs}
            dataset.generate_resource_from_iterator(
                hdrs, rows, htgs, self.folder, fn, rsdata
            )

        filename = "wfp_countries_global.csv"
        resourcedata = {
            "name": "Global WFP countries",
            "description": "Countries data with HXL tags including links to country datasets",
            "format": "csv",
        }
        dbtable_to_list(DBCountry, filename, resourcedata)
        filename = "wfp_commodities_global.csv"
        resourcedata = {
            "name": "Global WFP commodities",
            "description": "Commodities data with HXL tags",
            "format": "csv",
        }
        dbtable_to_list(DBCommodity, filename, resourcedata)
        filename = "wfp_markets_global.csv"
        resourcedata = {
            "name": "Global WFP markets",
            "description": "Markets data with HXL tags",
            "format": "csv",
        }
        dbtable_to_list(DBMarket, filename, resourcedata)
        dataset.set_reference_period(start_date, end_date)
        return dataset, showcase
