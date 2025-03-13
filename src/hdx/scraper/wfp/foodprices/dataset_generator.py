import logging
from typing import Dict, List, Optional, Tuple

from slugify import slugify
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from database.dbcommodity import DBCommodity
from database.dbcountry import DBCountry
from database.dbmarket import DBMarket

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.location.currency import Currency, CurrencyError
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.source_processing import process_source
from hdx.utilities.dateparse import default_date, default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        folder: str,
        session: Session,
        iso3_to_showcase_url: Dict[str, str],
        iso3_to_source: Dict[str, str],
        commodity_to_category: Dict[str, str],
    ):
        self.configuration = configuration
        self.folder = folder
        self.session = session
        self.iso3_to_showcase_url = iso3_to_showcase_url
        self.iso3_to_source = iso3_to_source
        self.commodity_to_category = commodity_to_category

    def get_dataset_and_showcase(
        self, countryiso3: str
    ) -> Tuple[Optional[Dataset], Optional[Showcase]]:
        if countryiso3 == "global":
            location = "world"
            countryname = "Global"
            name = "Global WFP food prices"
            url = "https://dataviz.vam.wfp.org/economic/prices"
        else:
            location = countryiso3
            countryname = Country.get_country_name_from_iso3(countryiso3)
            name = f"WFP food prices for {countryname}"
            url = self.iso3_to_showcase_url.get(countryiso3)
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

        dataset.set_expected_update_frequency("Every month")
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
        if not url:
            return dataset, None
        showcase = Showcase(
            {
                "name": f"{slugified_name}-showcase",
                "title": f"{title} showcase",
                "notes": f"{countryname} food prices data from World Food Programme displayed through VAM Economic Explorer",
                "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                "url": url,
            }
        )
        showcase.add_tags(tags)

        return dataset, showcase

    def generate_dataset_and_showcase(
        self,
        countryiso3: str,
        wfp_api: WFPAPI,
    ) -> Tuple[Optional[Dataset], Optional[Showcase], Optional[List]]:
        dataset, showcase = self.get_dataset_and_showcase(countryiso3)
        if not dataset:
            return None, None, None
        prices_data = wfp_api.get_items(
            "MarketPrices/PriceMonthly", countryiso3
        )
        if not prices_data:
            logger.info(f"{countryiso3} has no prices data!")
            return None, None, None
        market_to_adm = {}
        dbmarkets = []
        for market in wfp_api.get_items("Markets/List", countryiso3):
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
        rows = {}
        sources = {}
        markets = {}
        for price_data in prices_data:
            priceflag = price_data["commodityPriceFlag"]
            if not all(
                x in ("actual", "aggregate") for x in priceflag.split(",")
            ):
                continue
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

            process_source(sources, price_data["commodityPriceSourceName"])
            date_str = price_data["commodityPriceDate"]
            date = parse_date(date_str)
            date_str = date.date().isoformat()
            commodity = price_data["commodityName"]
            unit = price_data["commodityUnitName"]
            pricetype = price_data["priceTypeName"]
            price = price_data["commodityPrice"]
            currency = price_data["currencyName"]
            currency = self.configuration["currency_mappings"].get(
                currency, currency
            )
            try:
                usdprice = Currency.get_historic_value_in_usd(
                    price, currency, date
                )
                usdprice = round(usdprice, 4)
            except (CurrencyError, ZeroDivisionError):
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
                commodities = markets.get(adm1adm2market, {})
                dict_of_lists_add(
                    commodities,
                    (commodity, unit, pricetype, currency),
                    (date_str, usdprice),
                )
                markets[adm1adm2market] = commodities
        if not rows:
            logger.info(f"{countryiso3} has no prices!")
            return None, None, None
        logger.info(
            f"{len(rows)} unique prices rows of price type actual or aggregate"
        )
        number_market = []
        for key, commodities in markets.items():
            number_market.append((len(commodities), key))
        number_market = sorted(number_market, reverse=True)
        qc_indicators = []
        qc_hxltags = self.configuration["qc_hxltags"]
        qc_rows = [qc_hxltags]
        chosen_commodities = set()
        # Go through markets starting with the one with most commodities
        for _, adm1adm2market in number_market:
            commodities = markets[adm1adm2market]
            number_commodity = []
            for commodityunitpricetypecurrency, details in commodities.items():
                number_commodity.append(
                    (len(details), commodityunitpricetypecurrency)
                )
            number_commodity = sorted(number_commodity, reverse=True)
            index = 0
            # Pick commodity with most rows that has not already been used for another market
            commodity, unit, pricetype, currency = number_commodity[index][1]
            while commodity in chosen_commodities:
                index += 1
                if index == len(number_commodity):
                    commodity, unit, pricetype, currency = number_commodity[0][
                        1
                    ]
                    break
                commodity, unit, pricetype, currency = number_commodity[index][
                    1
                ]
            adm1, adm2, market_name = adm1adm2market
            code = f"{adm1}-{adm2}-{market_name}-{commodity}-{unit}-{pricetype}-{currency}"
            for date, usdprice in sorted(
                commodities[(commodity, unit, pricetype, currency)]
            ):
                qc_rows.append(
                    {"date": date, "code": code, "usdprice": usdprice}
                )
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
        source_override = self.iso3_to_source.get(countryiso3)
        if source_override is None:
            dataset["dataset_source"] = ", ".join(sorted(sources.values()))
        else:
            dataset["dataset_source"] = source_override
        filename = f"wfp_food_prices_{countryiso3.lower()}.csv"
        resourcedata = {
            "name": dataset["title"],
            "description": "Food prices data with HXL tags",
            "format": "csv",
        }
        rows = [rows[key] for key in sorted(rows)]
        hxltags = self.configuration["hxltags"]
        country_headers = self.configuration["country_headers"]
        country_hxltags = {
            header: hxltags[header] for header in country_headers
        }
        dataset.generate_resource_from_iterable(
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
            "name": f"QuickCharts: {dataset['title']}",
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
        dataset_date = dataset.get_time_period()
        self.session.execute(
            delete(DBCountry).where(DBCountry.countryiso3 == countryiso3)
        )
        self.session.execute(
            delete(DBMarket).where(DBMarket.countryiso3 == countryiso3)
        )
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

    def generate_global_dataset_and_showcase(
        self,
    ) -> Tuple[Optional[Dataset], Optional[Showcase]]:
        dataset, showcase = self.get_dataset_and_showcase("global")
        dataset["dataset_source"] = "WFP"

        hxltags = self.configuration["hxltags"]
        start_date = default_enddate
        end_date = default_date

        def dbtable_to_list(cls, fn, rsdata):
            nonlocal start_date, end_date
            rows = []
            for result in self.session.scalars(select(cls)).all():
                row = {}
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
            dataset.generate_resource_from_iterable(
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
        dataset.set_time_period(start_date, end_date)
        return dataset, showcase
