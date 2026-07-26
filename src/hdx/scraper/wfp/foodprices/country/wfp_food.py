import logging
from datetime import UTC

from hdx.api.configuration import Configuration
from hdx.location.currency import Currency, CurrencyError
from hdx.location.wfp_api import WFPAPI
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
)
from hdx.utilities.text import number_format

from hdx.scraper.wfp.foodprices.country.source_processing import process_source

logger = logging.getLogger(__name__)


class WFPFood:
    def __init__(
        self,
        countryiso3: str,
        configuration: Configuration,
        showcase_url: str | None,
        source: str | None,
        commodity_to_category: dict[str, str],
    ):
        self._countryiso3 = countryiso3
        self._configuration = configuration
        self._showcase_url = showcase_url
        self._source = source
        self._commodity_to_category = commodity_to_category
        self._prices_data = []
        self._markets = {}

    def get_price_markets(self, wfp_api: WFPAPI) -> bool:
        prices_data = wfp_api.get_market_prices_monthly(countryiso3=self._countryiso3)
        if not prices_data:
            logger.info(f"{self._countryiso3} has no prices data!")
            return False
        self._prices_data = prices_data
        for market in wfp_api.get_markets(countryiso3=self._countryiso3):
            market_id = market.market_id
            market_name = market.market_name
            admin1 = market.admin1_name
            admin2 = market.admin2_name
            latitude = market.market_latitude
            longitude = market.market_longitude
            self._markets[market_id] = (
                market_name,
                admin1,
                admin2,
                number_format(latitude, format="%.2f", trailing_zeros=False),
                number_format(longitude, format="%.2f", trailing_zeros=False),
            )
        logger.info(f"{len(prices_data)} prices rows")
        return True

    def generate_rows(self) -> tuple[dict, dict, dict]:
        prices_info = {}
        prices = {}
        prices_info["prices"] = prices
        sources = {}
        start_date = default_enddate
        end_date = default_date
        for price_data in self._prices_data:
            priceflag = price_data.commodity_price_flag
            if not all(x in ("actual", "aggregate") for x in priceflag.split(",")):
                continue
            commodity_id = price_data.commodity_id
            category = self._commodity_to_category[commodity_id]
            market_id = price_data.market_id
            result = self._markets.get(market_id)
            if result:
                market_name, adm1, adm2, lat, lon = result
            else:
                adm1 = adm2 = lat = lon = ""
                market_name = price_data.market_name
                self._markets[market_id] = market_name, adm1, adm2, lat, lon

            process_source(sources, price_data.commodity_price_source_name)
            date = price_data.commodity_price_date
            if date.tzinfo is None:
                # data_bridges_client parses WFP's timezone-less date strings
                # into naive datetimes; assume UTC to match previous
                # parse_date-based behaviour
                date = date.replace(tzinfo=UTC)
            if date < start_date:
                start_date = date
            if date > end_date:
                end_date = date
            date_str = iso_string_from_datetime(date)
            commodity = price_data.commodity_name
            unit = price_data.commodity_unit_name
            pricetype = price_data.price_type_name
            price = price_data.commodity_price
            currency = price_data.currency_name
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
        if prices:
            logger.info(
                f"{len(prices)} unique prices rows of price type actual or aggregate"
            )
        else:
            logger.info(f"{self._countryiso3} has no prices!")
        prices_info["start_date"] = start_date
        prices_info["end_date"] = end_date
        return prices_info, self._markets, sources
