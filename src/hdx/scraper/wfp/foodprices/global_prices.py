import logging
from glob import iglob
from typing import Dict

from dateutil.relativedelta import relativedelta

from hdx.utilities.dateparse import default_date, default_enddate, parse_date
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


def get_global_prices_rows(
    downloader: Download,
    folder: str,
    years: int = 2,
) -> Dict:
    filepaths = []
    for filepath in iglob(f"{folder}/wfp_food_prices*.csv", recursive=False):
        if any(x in filepath for x in ("_global", "_qc")):
            continue
        filepaths.append(filepath)

    earliest_date = default_enddate
    latest_date = default_date
    global_rows = []
    for filepath in sorted(filepaths):
        countryiso3 = filepath[-7:-4].upper()
        headers, iterator = downloader.get_tabular_rows(
            filepath, has_hxl=True, dict_form=True
        )
        logger.info(f"Reading from {countryiso3}: {filepath}")
        end_date = default_date
        country_rows = []
        for row in iterator:
            date = row["date"]
            if date[0] == "#":
                continue
            date = parse_date(date)
            row["parsed_date"] = date
            if date > end_date:
                end_date = date
            country_rows.append(row)
        total_country_rows = len(country_rows)
        logger.info(f"Read {total_country_rows}")
        if not total_country_rows:
            continue
        start_date = end_date - relativedelta(years=years)
        subset_country_rows = 0
        for row in country_rows:
            date = row["parsed_date"]
            if date < start_date:
                continue
            if date < earliest_date:
                earliest_date = date
            if date > latest_date:
                latest_date = date
            del row["parsed_date"]
            row["countryiso3"] = countryiso3
            subset_country_rows += 1
            global_rows.append(row)
        del country_rows
        logger.info(f"Taking {subset_country_rows} from {start_date} to {end_date}")
    number_rows = len(global_rows)
    logger.info(f"Total rows for all countries: {number_rows}")
    info = {
        "rows": global_rows,
        "start_date": earliest_date,
        "end_date": latest_date,
    }
    return info
