import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dateutil.relativedelta import relativedelta
from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.scraper.wfp.foodprices.utilities import round_min_digits
from hdx.utilities.text import number_format

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        now: datetime,
        configuration: Configuration,
        folder: str,
        iso3_to_showcase_url: Dict[str, str],
        iso3_to_source: Dict[str, str],
        currencies: List[Dict],
        years: int = 5,
    ):
        self._start_date = now - relativedelta(years=years)
        self._configuration = configuration
        self._folder = folder
        self._iso3_to_showcase_url = iso3_to_showcase_url
        self._iso3_to_source = iso3_to_source
        self._currencies = currencies

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
            url = self._iso3_to_showcase_url.get(countryiso3)
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

    def complete_dataset(
        self,
        countryiso3: str,
        dataset: Dataset,
        rows: Dict,
        markets: Dict,
        sources: Dict,
    ) -> Tuple[Dataset, List, List]:
        number_market = []
        for key, commodities in markets.items():
            number_market.append((len(commodities), key))
        number_market = sorted(number_market, reverse=True)
        qc_indicators = []
        qc_hxltags = self._configuration["qc_hxltags"]
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
                    {
                        "date": date,
                        "code": code,
                        "usdprice": round_min_digits(usdprice),
                    }
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
        source_override = self._iso3_to_source.get(countryiso3)
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
        hxltags = self._configuration["hxltags"]
        country_headers = self._configuration["country_headers"]
        country_hxltags = {
            header: hxltags[header] for header in country_headers
        }
        dbprices = []

        def get_rows():
            for key in sorted(rows):
                (
                    priceflag,
                    date,
                    adm1,
                    adm2,
                    market_name,
                    category,
                    commodity,
                    unit,
                    pricetype,
                ) = key
                (
                    date_str,
                    market_id,
                    lat,
                    lon,
                    commodity_id,
                    currency,
                    price,
                    usdprice,
                ) = rows[key]
                # Only add rows in last N years
                if date > self._start_date:
                    dbprices.append(
                        {
                            "countryiso3": countryiso3,
                            "date": date,
                            "market_id": market_id,
                            "commodity_id": commodity_id,
                            "unit": unit,
                            "priceflag": priceflag,
                            "pricetype": pricetype,
                            "currency": currency,
                            "price": price,
                            "usdprice": usdprice,
                        }
                    )
                yield {
                    "date": date_str,
                    "admin1": adm1,
                    "admin2": adm2,
                    "market": market_name,
                    "latitude": number_format(
                        lat, format="%.2f", trailing_zeros=False
                    ),
                    "longitude": number_format(
                        lon, format="%.2f", trailing_zeros=False
                    ),
                    "category": category,
                    "commodity": commodity,
                    "unit": unit,
                    "priceflag": priceflag,
                    "pricetype": pricetype,
                    "currency": currency,
                    "price": number_format(
                        price, format="%.2f", trailing_zeros=False
                    ),
                    "usdprice": round_min_digits(usdprice),
                }

        dataset.generate_resource_from_iterable(
            country_headers,
            get_rows(),
            country_hxltags,
            self._folder,
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
            self._folder,
            filename,
            qc_rows,
            resourcedata,
            headers=list(qc_hxltags.keys()),
        )
        return dataset, qc_indicators, dbprices

    def generate_global_dataset_and_showcase(
        self, table_data: Dict, start_date: datetime, end_date: datetime
    ) -> Tuple[Optional[Dataset], Optional[Showcase]]:
        dataset, showcase = self.get_dataset_and_showcase("global")
        dataset["dataset_source"] = "WFP"
        dataset.set_time_period(start_date, end_date)

        filename = "wfp_food_prices_global.csv"
        resourcedata = {
            "name": "Global WFP food prices",
            "description": "Last 5 years of prices data with HXL tags",
            "format": "csv",
        }
        info = table_data["DBPrice"]
        dataset.generate_resource_from_iterable(
            info["headers"],
            info["rows"],
            info["hxltags"],
            self._folder,
            filename,
            resourcedata,
        )
        filename = "wfp_countries_global.csv"
        resourcedata = {
            "name": "Global WFP countries",
            "description": "Countries data with HXL tags with links to country datasets containing all available historic data",
            "format": "csv",
        }
        info = table_data["DBCountry"]
        dataset.generate_resource_from_iterable(
            info["headers"],
            info["rows"],
            info["hxltags"],
            self._folder,
            filename,
            resourcedata,
        )

        filename = "wfp_commodities_global.csv"
        resourcedata = {
            "name": "Global WFP commodities",
            "description": "Commodities data with HXL tags",
            "format": "csv",
        }
        info = table_data["DBCommodity"]
        dataset.generate_resource_from_iterable(
            info["headers"],
            info["rows"],
            info["hxltags"],
            self._folder,
            filename,
            resourcedata,
        )

        filename = "wfp_markets_global.csv"
        resourcedata = {
            "name": "Global WFP markets",
            "description": "Markets data with HXL tags",
            "format": "csv",
        }
        info = table_data["DBMarket"]
        dataset.generate_resource_from_iterable(
            info["headers"],
            info["rows"],
            info["hxltags"],
            self._folder,
            filename,
            resourcedata,
        )

        filename = "wfp_currencies_global.csv"
        resourcedata = {
            "name": "Global WFP currencies",
            "description": "Currencies data with HXL tags",
            "format": "csv",
        }
        currency_hxltags = self._configuration["currency_hxltags"]
        dataset.generate_resource_from_iterable(
            list(currency_hxltags.keys()),
            self._currencies,
            currency_hxltags,
            self._folder,
            filename,
            resourcedata,
        )
        return dataset, showcase
