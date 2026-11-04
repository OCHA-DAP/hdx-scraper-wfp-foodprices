import csv
import logging
from contextlib import ExitStack
from datetime import datetime
from glob import iglob
from os.path import join

from hdx.api.configuration import Configuration
from hdx.utilities.dateparse import default_date, default_enddate, parse_date

logger = logging.getLogger(__name__)


class GlobalPricesGenerator:
    filename = "wfp_food_prices_global_{}.csv"

    def __init__(self, configuration: Configuration, folder: str):
        self._configuration = configuration
        self._folder = folder

    def create_prices_files(
        self, output_dir: str = ""
    ) -> tuple[datetime, datetime, dict]:
        # Dates are YYYY-MM-DD so the year is the first 4 characters and dates
        # compare as strings
        if not output_dir:
            output_dir = self._folder
        prices_headers = ["countryiso3", *self._configuration["prices_headers"]]
        prices_paths = sorted(
            filepath
            for filepath in iglob(f"{self._folder}/wfp_food_prices*.csv")
            if "_global" not in filepath
        )
        earliest_date = None
        latest_date = None
        year_to_path = {}
        year_to_writer = {}
        with ExitStack() as stack:
            for filepath in prices_paths:
                countryiso3 = filepath[-7:-4].upper()
                logger.info(f"Reading prices from {countryiso3}: {filepath}")
                with open(filepath, encoding="utf-8", newline="") as f:
                    for row in csv.DictReader(f):
                        date = row["date"]
                        if earliest_date is None or date < earliest_date:
                            earliest_date = date
                        if latest_date is None or date > latest_date:
                            latest_date = date
                        year = int(date[:4])
                        writer = year_to_writer.get(year)
                        if writer is None:
                            path = join(output_dir, self.filename.format(year))
                            output = stack.enter_context(
                                open(path, "w", encoding="utf-8", newline="")
                            )
                            writer = csv.DictWriter(
                                output, prices_headers, extrasaction="ignore"
                            )
                            writer.writeheader()
                            year_to_writer[year] = writer
                            year_to_path[year] = path
                        row["countryiso3"] = countryiso3
                        writer.writerow(row)
        if earliest_date is None:
            return default_enddate, default_date, year_to_path
        return parse_date(earliest_date), parse_date(latest_date), year_to_path
