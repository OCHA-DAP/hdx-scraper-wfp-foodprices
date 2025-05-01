import logging
from glob import iglob
from typing import Dict, List, Optional

from hdx.utilities.dateparse import default_date, default_enddate, parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


class FileReader:
    def __init__(self, downloader: Download, folder: str):
        self._downloader = downloader
        self._folder = folder

    def get_global_prices(self) -> Optional[Dict]:
        filepaths = []
        for filepath in iglob(f"{self._folder}/wfp_food_prices*.csv", recursive=False):
            if any(x in filepath for x in ("_global", "_qc")):
                continue
            filepaths.append(filepath)

        earliest_date = default_enddate
        latest_date = default_date
        rows_by_year = {}
        total_global_rows = 0
        for filepath in sorted(filepaths):
            countryiso3 = filepath[-7:-4].upper()
            _, iterator = self._downloader.get_tabular_rows(
                filepath, has_hxl=True, dict_form=True, encoding="utf-8"
            )
            logger.info(f"Reading from {countryiso3}: {filepath}")
            total_country_rows = 0
            for row in iterator:
                date = row["date"]
                if date[0] == "#":
                    continue
                date = parse_date(date)
                if date < earliest_date:
                    earliest_date = date
                if date > latest_date:
                    latest_date = date
                row["countryiso3"] = countryiso3
                total_country_rows += 1
                dict_of_lists_add(rows_by_year, date.year, row)
            logger.info(f"Read {total_country_rows}")
            total_global_rows += total_country_rows
        logger.info(f"Total rows for all countries: {total_global_rows}")
        if total_global_rows == 0:
            return None
        return {
            "rows_by_year": rows_by_year,
            "start_date": earliest_date,
            "end_date": latest_date,
        }

    def get_global_markets(self) -> Optional[List]:
        filepaths = []
        for filepath in iglob(f"{self._folder}/wfp_markets*.csv", recursive=False):
            if any(x in filepath for x in ("_global",)):
                continue
            filepaths.append(filepath)

        rows = []
        for filepath in sorted(filepaths):
            countryiso3 = filepath[-7:-4].upper()
            _, iterator = self._downloader.get_tabular_rows(
                filepath, has_hxl=True, dict_form=True, encoding="utf-8"
            )
            logger.info(f"Reading from {countryiso3}: {filepath}")

            for row in iterator:
                market_id = row["market_id"]
                if market_id[0] == "#":
                    continue
                rows.append(row)
        if len(rows) == 0:
            return None
        return rows
