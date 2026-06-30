import logging

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.hdxobject import HDXError
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.text import number_format
from slugify import slugify

from hdx.scraper.wfp.foodprices.utilities import round_min_digits

logger = logging.getLogger(__name__)


class DatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        folder: str,
        iso3_to_showcase_url: dict[str, str],
        iso3_to_source: dict[str, str],
        currencies: list[dict],
    ):
        self._configuration = configuration
        self._folder = folder
        self._iso3_to_showcase_url = iso3_to_showcase_url
        self._iso3_to_source = iso3_to_source
        self._currencies = currencies

    def get_dataset_and_showcase(
        self, countryiso3: str
    ) -> tuple[Dataset | None, Showcase | None]:
        countryname = Country.get_country_name_from_iso3(countryiso3)
        name = f"WFP food prices for {countryname}"
        slugified_name = slugify(name).lower()
        url = self._iso3_to_showcase_url.get(countryiso3)
        title = f"{countryname} - Food Prices"
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
            dataset.add_country_location(countryiso3)
        except HDXError as e:
            logger.exception(f"{countryname} has a problem! {e}")
            return None, None
        dataset.set_subnational(True)
        tags = ("economics", "food security", "indicators", "markets")
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
        prices_info: dict,
        markets: dict,
        sources: dict,
    ) -> Dataset:
        dataset.set_time_period(prices_info["start_date"], prices_info["end_date"])
        source_override = self._iso3_to_source.get(countryiso3)
        if source_override is None:
            dataset["dataset_source"] = ", ".join(sorted(sources.values()))
        else:
            dataset["dataset_source"] = source_override

        countryiso3_lower = countryiso3.lower()
        filename = f"wfp_food_prices_{countryiso3_lower}.csv"
        dataset_title = dataset["title"]
        resourcedata = {
            "name": dataset_title,
            "description": "Food prices data",
            "format": "csv",
        }
        prices_headers = self._configuration["prices_headers"]
        rows = []
        prices = prices_info["prices"]
        for key in sorted(prices):
            (
                priceflag,
                date_str,
                adm1,
                adm2,
                market_name,
                category,
                commodity,
                unit,
                pricetype,
            ) = key
            (
                market_id,
                lat,
                lon,
                commodity_id,
                currency,
                price,
                usdprice,
            ) = prices[key]
            rows.append(
                {
                    "date": date_str,
                    "admin1": adm1,
                    "admin2": adm2,
                    "market": market_name,
                    "market_id": market_id,
                    "latitude": lat,
                    "longitude": lon,
                    "category": category,
                    "commodity": commodity,
                    "commodity_id": commodity_id,
                    "unit": unit,
                    "priceflag": priceflag,
                    "pricetype": pricetype,
                    "currency": currency,
                    "price": number_format(price, format="%.2f", trailing_zeros=False),
                    "usdprice": round_min_digits(usdprice),
                }
            )
        dataset.generate_resource(
            self._folder,
            filename,
            rows,
            resourcedata,
            headers=prices_headers,
        )

        filename = f"wfp_markets_{countryiso3_lower}.csv"
        resourcedata = {
            "name": dataset_title.replace("Food Prices", "Markets"),
            "description": "Markets data",
            "format": "csv",
        }
        markets_headers = self._configuration["markets_headers"]
        rows = []
        for market_id in sorted(markets):
            market_name, adm1, adm2, lat, lon = markets[market_id]
            rows.append(
                {
                    "market_id": market_id,
                    "market": market_name,
                    "countryiso3": countryiso3,
                    "admin1": adm1,
                    "admin2": adm2,
                    "latitude": lat,
                    "longitude": lon,
                }
            )
        dataset.generate_resource(
            self._folder,
            filename,
            rows,
            resourcedata,
            headers=markets_headers,
        )
        return dataset
