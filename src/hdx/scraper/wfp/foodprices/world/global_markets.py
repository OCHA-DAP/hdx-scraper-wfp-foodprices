import logging
from glob import iglob
from typing import List, Optional

from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)


def get_markets(downloader: Download, folder: str) -> Optional[List]:
    filepaths = []
    for filepath in iglob(f"{folder}/wfp_markets*.csv", recursive=False):
        if any(x in filepath for x in ("_global",)):
            continue
        filepaths.append(filepath)

    rows = []
    for filepath in sorted(filepaths):
        countryiso3 = filepath[-7:-4].upper()
        _, iterator = downloader.get_tabular_rows(
            filepath, has_hxl=True, dict_form=True, encoding="utf-8"
        )
        logger.info(f"Reading markets from {countryiso3}: {filepath}")

        for row in iterator:
            market_id = row["market_id"]
            if market_id[0] == "#":
                continue
            rows.append(row)
    if len(rows) == 0:
        return None
    return rows
