import logging
from datetime import datetime, timezone
from glob import iglob
from os.path import join
from typing import Dict, Tuple

from hdx.api.configuration import Configuration
from hdx.utilities.dateparse import default_date, default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_sets_add, write_list_to_csv
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


class GlobalPricesGenerator:
    filename = "wfp_food_prices_global_{}.csv"

    def __init__(self, configuration: Configuration, downloader: Download, folder: str):
        self._configuration = configuration
        self._downloader = downloader
        self._folder = folder
        self._prices_paths = {}
        self._years = None
        self._year_to_countries = {}

    def get_years_per_country(self) -> Tuple[datetime, datetime]:
        for filepath in sorted(
            iglob(f"{self._folder}/wfp_food_prices*.csv", recursive=False)
        ):
            if any(x in filepath for x in ("_global", "_qc")):
                continue
            countryiso3 = filepath[-7:-4].upper()
            self._prices_paths[countryiso3] = filepath
        earliest_date = default_enddate
        latest_date = default_date
        years = set()
        for countryiso3, filepath in self._prices_paths.items():
            _, iterator = self._downloader.get_tabular_rows(
                filepath, has_hxl=True, dict_form=True, encoding="utf-8"
            )
            logger.info(f"Reading year info from {countryiso3}: {filepath}")
            for row in iterator:
                date = row["date"]
                if date[0] == "#":
                    continue
                date = parse_date(date)
                if date < earliest_date:
                    earliest_date = date
                if date > latest_date:
                    latest_date = date
                years.add(date.year)
                dict_of_sets_add(self._year_to_countries, date.year, countryiso3)
        self._years = sorted(years, reverse=True)
        return earliest_date, latest_date

    def create_prices_files(self, output_dir: str = "") -> Dict:
        year_to_path = {}

        prices_headers = self._configuration["prices_headers"]
        prices_headers.insert(0, "countryiso3")
        hxltags = self._configuration["hxltags"]
        prices_hxltags = {header: hxltags[header] for header in prices_headers}

        for year in self._years:
            logger.info(f"Processing {year} prices")
            startdate = datetime(year, 1, 1, tzinfo=timezone.utc)
            enddate = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
            rows = [Download.hxl_row(prices_headers, prices_hxltags, dict_form=True)]
            for countryiso3 in sorted(self._year_to_countries[year]):
                filepath = self._prices_paths[countryiso3]
                _, iterator = self._downloader.get_tabular_rows(
                    filepath, has_hxl=True, dict_form=True, encoding="utf-8"
                )
                for row in iterator:
                    date = row["date"]
                    if date[0] == "#":
                        continue
                    date = parse_date(date)
                    if date < startdate or date > enddate:
                        continue
                    row["countryiso3"] = countryiso3
                    rows.append(row)
            if len(rows) == 1:
                continue
            if not output_dir:
                output_dir = self._folder
            filename = self.filename.format(year)
            filepath = join(output_dir, filename)
            write_list_to_csv(filepath, rows, columns=prices_headers)
            year_to_path[year] = filepath
        return year_to_path
