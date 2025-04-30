import logging
from typing import Dict, List, Optional, Tuple

from slugify import slugify

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase

logger = logging.getLogger(__name__)


class DatasetGenerator:
    global_prices_name = "Global WFP food prices"
    slugified_global_name = slugify(global_prices_name).lower()
    global_markets_name = "Global WFP markets"
    global_commodities_name = "Global WFP commodities"
    global_currencies_name = "Global WFP currencies"

    def __init__(
        self,
        configuration: Configuration,
        folder: str,
        currencies: List[Dict],
    ):
        self._configuration = configuration
        self._folder = folder
        self._currencies = currencies

    def get_dataset_and_showcase(
        self
    ) -> Tuple[Optional[Dataset], Optional[Showcase]]:
        location = "world"
        location_name = "Global"
        slugified_name = self.slugified_global_name
        url = "https://dataviz.vam.wfp.org/economic/prices"
        title = f"{location_name} - Food Prices"
        logger.info(f"Creating dataset: {title}")

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
            dataset.add_other_location(location)
        except HDXError as e:
            logger.exception(f"{location_name} has a problem! {e}")
            return None, None
        dataset.set_subnational(True)
        tags = ("hxl", "economics", "food security", "indicators", "markets")
        dataset.add_tags(tags)
        showcase = Showcase(
            {
                "name": f"{slugified_name}-showcase",
                "title": f"{title} showcase",
                "notes": f"{location_name} food prices data from World Food Programme displayed through VAM Economic Explorer",
                "image_url": "https://dataviz.vam.wfp.org/images/overview-image.jpg",
                "url": url,
            }
        )
        showcase.add_tags(tags)
        return dataset, showcase

    def generate_global_dataset_and_showcase(
        self,
        global_prices_info: Dict,
        markets: List[Dict],
        commodities: List[Dict],
    ) -> Tuple[Optional[Dataset], Optional[Showcase]]:
        dataset, showcase = self.get_dataset_and_showcase()
        hxltags = self._configuration["hxltags"]
        start_date = global_prices_info["start_date"]
        end_date = global_prices_info["end_date"]
        dataset.set_time_period(start_date, end_date)
        dataset["dataset_source"] = "WFP"

        rows_by_year = global_prices_info["rows_by_year"]
        for year in sorted(rows_by_year, reverse=True):
            filename = f"wfp_food_prices_global_{year}.csv"
            resourcedata = {
                "name": f"{self.global_prices_name} {year}",
                "description": f"Prices data for {year} with HXL tags",
                "format": "csv",
            }
            prices_headers = self._configuration["prices_headers"]
            prices_headers.insert(0, "countryiso3")
            prices_hxltags = {header: hxltags[header] for header in prices_headers}
            dataset.generate_resource_from_iterable(
                prices_headers,
                rows_by_year[year],
                prices_hxltags,
                self._folder,
                filename,
                resourcedata,
            )

        filename = "wfp_commodities_global.csv"
        resourcedata = {
            "name": self.global_commodities_name,
            "description": "Commodities data with HXL tags",
            "format": "csv",
        }
        commodities_headers = self._configuration["commodities_headers"]
        commodities_hxltags = {header: hxltags[header] for header in commodities_headers}
        dataset.generate_resource_from_iterable(
            commodities_headers,
            sorted(commodities, key=lambda x: x["commodity_id"]),
            commodities_hxltags,
            self._folder,
            filename,
            resourcedata,
        )

        filename = "wfp_markets_global.csv"
        resourcedata = {
            "name": self.global_markets_name,
            "description": "Markets data with HXL tags",
            "format": "csv",
        }
        markets_headers = self._configuration["markets_headers"]
        markets_hxltags = {header: hxltags[header] for header in markets_headers}
        dataset.generate_resource_from_iterable(
            markets_headers,
            sorted(markets, key=lambda x: int(x["market_id"])),
            markets_hxltags,
            self._folder,
            filename,
            resourcedata,
        )

        filename = "wfp_currencies_global.csv"
        resourcedata = {
            "name": self.global_currencies_name,
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
