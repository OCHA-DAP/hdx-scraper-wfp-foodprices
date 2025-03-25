import logging
from datetime import datetime
from glob import iglob
from typing import Generator

from dateutil.relativedelta import relativedelta

from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


def get_global_prices_rows(
    now: datetime,
    retriever: Retrieve,
    folder: str,
    years: int = 5,
) -> Generator:
    start_date = now - relativedelta(years=years)

    filepaths = []
    for filepath in iglob(f"{folder}/wfp_food_prices*.csv", recursive=False):
        if any(x in filepath for x in ("_global", "_qc")):
            continue
        filepaths.append(filepath)

    for filepath in sorted(filepaths):
        countryiso3 = filepath[-7:-4].upper()
        headers, iterator = retriever.downloader.get_tabular_rows(
            filepath, has_hxl=True, dict_form=True
        )
        row_count = 0
        total_count = 0
        for row in iterator:
            date = row["date"]
            if date[0] == "#":
                continue
            total_count += 1
            if parse_date(date) < start_date:
                continue
            row["countryiso3"] = countryiso3
            row_count += 1
            yield row
        logger.info(
            f"Taking {row_count} of {total_count} rows from {countryiso3}: {filepath}"
        )
