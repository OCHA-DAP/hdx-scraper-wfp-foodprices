import logging
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.location.currency import Currency, CurrencyError
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.source_processing import process_source
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


class WFPFood:
    def __init__(
        self,
        countryiso3: str,
        configuration: Configuration,
        showcase_url: Optional[str],
        source: Optional[str],
        commodity_to_category: Dict[str, str],
    ):
        self._countryiso3 = countryiso3
        self._configuration = configuration
        self._showcase_url = showcase_url
        self._source = source
        self._commodity_to_category = commodity_to_category
        self._prices_data = []
        self._market_to_adm = {}

    def get_price_markets(self, wfp_api: WFPAPI) -> List[Dict]:
        dbmarkets = []
        prices_data = wfp_api.get_items("MarketPrices/PriceMonthly", self._countryiso3)
        if not prices_data:
            logger.info(f"{self._countryiso3} has no prices data!")
            return dbmarkets
        self._prices_data = prices_data
        for market in wfp_api.get_items("Markets/List", self._countryiso3):
            market_id = market["marketId"]
            market_name = market["marketName"]
            admin1 = market["admin1Name"]
            admin2 = market["admin2Name"]
            latitude = market["marketLatitude"]
            longitude = market["marketLongitude"]
            self._market_to_adm[market_id] = (
                admin1,
                admin2,
                latitude,
                longitude,
            )
            dbmarkets.append(
                {
                    "market_id": market_id,
                    "market": market_name,
                    "countryiso3": self._countryiso3,
                    "admin1": admin1,
                    "admin2": admin2,
                    "latitude": latitude,
                    "longitude": longitude,
                }
            )
        logger.info(f"{len(prices_data)} prices rows")
        return dbmarkets

    def generate_rows(self, dbmarkets: List[Dict]) -> Tuple[Dict, Dict, Dict]:
        rows = {}
        markets = {}
        sources = {}
        for price_data in self._prices_data:
            priceflag = price_data["commodityPriceFlag"]
            if not all(x in ("actual", "aggregate") for x in priceflag.split(",")):
                continue
            commodity_id = price_data["commodityID"]
            category = self._commodity_to_category[commodity_id]
            market_id = price_data["marketID"]
            market_name = price_data["marketName"]
            result = self._market_to_adm.get(market_id)
            if result:
                adm1, adm2, lat, lon = result
            else:
                adm1 = adm2 = lat = lon = ""
                self._market_to_adm[market_id] = adm1, adm2, lat, lon
                dbmarkets.append(
                    {
                        "market_id": market_id,
                        "market": market_name,
                        "countryiso3": self._countryiso3,
                    }
                )

            process_source(sources, price_data["commodityPriceSourceName"])
            date_str = price_data["commodityPriceDate"]
            date = parse_date(date_str)
            date_str = iso_string_from_datetime(date)
            commodity = price_data["commodityName"]
            unit = price_data["commodityUnitName"]
            pricetype = price_data["priceTypeName"]
            price = price_data["commodityPrice"]
            currency = price_data["currencyName"]
            currency = self._configuration["currency_mappings"].get(currency, currency)
            try:
                usdprice = Currency.get_historic_value_in_usd(price, currency, date)
            except (CurrencyError, ZeroDivisionError):
                usdprice = None
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
                rows[key] = (
                    date_str,
                    market_id,
                    lat,
                    lon,
                    commodity_id,
                    currency,
                    price,
                    usdprice,
                )
            if adm1 and adm2 and category and usdprice:
                adm1adm2market = adm1, adm2, market_name
                commodities = markets.get(adm1adm2market, {})
                dict_of_lists_add(
                    commodities,
                    (commodity, unit, pricetype, currency),
                    (date_str, usdprice),
                )
                markets[adm1adm2market] = commodities
        if rows:
            logger.info(
                f"{len(rows)} unique prices rows of price type actual or aggregate"
            )
        else:
            logger.info(f"{self._countryiso3} has no prices!")
        return rows, markets, sources
