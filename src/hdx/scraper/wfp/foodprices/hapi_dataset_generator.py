from logging import getLogger
from typing import Dict, Optional, Tuple

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

logger = getLogger(__name__)


class HAPIDatasetGenerator:
    def __init__(
        self,
        configuration: Configuration,
        global_markets_info: Dict,
        global_prices_info: Dict,
    ) -> None:
        self._configuration = configuration["hapi_dataset"]
        self.slugified_name = self._configuration["name"]
        self._global_markets_info = global_markets_info
        self._global_prices_info = global_prices_info

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
        dataset.set_expected_update_frequency("Every week")
        dataset.add_tags(self._configuration["tags"])
        dataset["dataset_source"] = self._configuration["dataset_source"]
        dataset["license_id"] = self._configuration["license_id"]
        dataset.set_subnational(True)

        resources_config = self._configuration["resources"]
        return dataset, resources_config

    def generate_prices_dataset(
        self,
        folder: str,
    ) -> Optional[Dataset]:
        rows = self._global_prices_info["rows"]
        if len(rows) == 0:
            logger.warning("Food prices has no data!")
            return None

        dataset, resources_config = self.generate_dataset()
        dataset.add_other_location("World")
        start_date = self._global_prices_info["start_date"]
        end_date = self._global_prices_info["end_date"]
        dataset.set_time_period(start_date, end_date)

        resource_config = resources_config[0]
        resource_name = resource_config["name"]
        resourcedata = {
            "name": resource_name,
            "description": resource_config["description"],
        }
        hxltags = resource_config["hxltags"]
        filename = resource_config["filename"]

        success, _ = dataset.generate_resource_from_iterable(
            list(hxltags.keys()),
            self._global_prices_info["rows"],
            hxltags,
            folder,
            f"{filename}.csv",
            resourcedata,
        )
        if success is False:
            logger.warning(f"{resource_name} has no data!")
            return None

        resource_config = resources_config[1]
        resource_name = resource_config["name"]
        resourcedata = {
            "name": resource_name,
            "description": resource_config["description"],
        }
        hxltags = resource_config["hxltags"]
        filename = resource_config["filename"]

        success, _ = dataset.generate_resource_from_iterable(
            list(hxltags.keys()),
            self._global_markets_info["rows"],
            hxltags,
            folder,
            f"{filename}.csv",
            resourcedata,
        )
        if success is False:
            logger.warning(f"{resource_name} has no data!")
            return None

        dataset.preview_off()
        return dataset
