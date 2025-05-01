import logging
from typing import Dict, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.location.currency import Currency, CurrencyError
from hdx.location.wfp_api import WFPAPI
from hdx.scraper.wfp.foodprices.country.source_processing import process_source
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
    parse_date,
)
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.text import number_format

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
        self._markets = {}

    def get_price_markets(self, wfp_api: WFPAPI) -> bool:
        prices_data = wfp_api.get_items("MarketPrices/PriceMonthly", self._countryiso3)
        if not prices_data:
            logger.info(f"{self._countryiso3} has no prices data!")
            return False
        self._prices_data = prices_data
        for market in wfp_api.get_items("Markets/List", self._countryiso3):
            market_id = market["marketId"]
            market_name = market["marketName"]
            admin1 = market["admin1Name"]
            admin2 = market["admin2Name"]
            latitude = market["marketLatitude"]
            longitude = market["marketLongitude"]
            self._markets[market_id] = (
                market_name,
                admin1,
                admin2,
                number_format(latitude, format="%.2f", trailing_zeros=False),
                number_format(longitude, format="%.2f", trailing_zeros=False),
            )
        logger.info(f"{len(prices_data)} prices rows")
        return True

    def generate_rows(self) -> Tuple[Dict, Dict, Dict, Dict]:
        prices_info = {}
        prices = {}
        prices_info["prices"] = prices
        market_to_commodities = {}
        sources = {}
        start_date = default_enddate
        end_date = default_date
        for price_data in self._prices_data:
            priceflag = price_data["commodityPriceFlag"]
            if not all(x in ("actual", "aggregate") for x in priceflag.split(",")):
                continue
            commodity_id = price_data["commodityID"]
            category = self._commodity_to_category[commodity_id]
            market_id = price_data["marketID"]
            result = self._markets.get(market_id)
            if result:
                market_name, adm1, adm2, lat, lon = result
            else:
                adm1 = adm2 = lat = lon = ""
                market_name = price_data["marketName"]
                self._markets[market_id] = market_name, adm1, adm2, lat, lon

            process_source(sources, price_data["commodityPriceSourceName"])
            date_str = price_data["commodityPriceDate"]
            date = parse_date(date_str)
            if date < start_date:
                start_date = date
            if date > end_date:
                end_date = date
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
                date_str,
                adm1,
                adm2,
                market_name,
                category,
                commodity,
                unit,
                pricetype,
            )
            if key not in prices:
                prices[key] = (
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
                commodities = market_to_commodities.get(adm1adm2market, {})
                dict_of_lists_add(
                    commodities,
                    (commodity, unit, pricetype, currency),
                    (date_str, usdprice),
                )
                market_to_commodities[adm1adm2market] = commodities
        if prices:
            logger.info(
                f"{len(prices)} unique prices rows of price type actual or aggregate"
            )
        else:
            logger.info(f"{self._countryiso3} has no prices!")
        prices_info["start_date"] = start_date
        prices_info["end_date"] = end_date
        return prices_info, self._markets, market_to_commodities, sources
