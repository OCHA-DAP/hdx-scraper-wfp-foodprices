from datetime import datetime
from logging import getLogger
from typing import Dict, List, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.data.resource import Resource

logger = getLogger(__name__)


class HAPIDatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        folder: str,
        start_date: datetime,
        end_date: datetime,
    ) -> None:
        self._configuration = configuration["hapi_dataset"]
        self.slugified_name = self._configuration["name"]
        self._folder = folder
        self._start_date = start_date
        self._end_date = end_date

    def generate_dataset(self) -> Tuple[Dataset, Dict]:
        title = self._configuration["title"]
        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": self.slugified_name,
                "title": title,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("40d10ece-49de-4791-9aed-e164f1d16dd1")
        dataset.set_expected_update_frequency("Every month")
        dataset.add_tags(self._configuration["tags"])
        dataset["dataset_source"] = self._configuration["dataset_source"]
        dataset["license_id"] = self._configuration["license_id"]
        dataset.set_subnational(True)

        resources_config = self._configuration["resources"]
        return dataset, resources_config

    def generate_prices_dataset(
        self,
        hapi_year_to_pricespath: Dict,
        hapi_markets: List[Dict],
        hapi_commodities: List[Dict],
        hapi_currencies: List[Dict],
    ) -> Optional[Dataset]:
        if not hapi_year_to_pricespath:
            logger.warning("Food prices has no data!")
            return None

        dataset, resources_config = self.generate_dataset()
        dataset.add_other_location("World")
        dataset.set_time_period(self._start_date, self._end_date)

        resource_config = resources_config[0]
        for year in sorted(hapi_year_to_pricespath, reverse=True):
            filepath = hapi_year_to_pricespath[year]
            resource_name = resource_config["name"]
            description = resource_config["description"]
            resourcedata = {
                "name": resource_name.format(year),
                "description": f"{year} {description}",
            }
            resource = Resource(resourcedata)
            resource.set_format("csv")
            resource.set_file_to_upload(filepath)
            dataset.add_update_resource(resource)

        for i, rows in enumerate((hapi_markets, hapi_commodities, hapi_currencies)):
            resource_config = resources_config[i + 1]
            resource_name = resource_config["name"]
            resourcedata = {
                "name": resource_name,
                "description": resource_config["description"],
            }
            hxltags = resource_config["hxltags"]
            filename = resource_config["filename"]

            success, _ = dataset.generate_resource_from_iterable(
                list(hxltags.keys()),
                rows,
                hxltags,
                self._folder,
                f"{filename}.csv",
                resourcedata,
            )
            if success is False:
                logger.warning(f"{resource_name} has no data!")
                return None

        dataset.preview_off()
        return dataset
