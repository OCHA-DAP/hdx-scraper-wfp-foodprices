import logging
from datetime import datetime
from os.path import exists, join
from typing import Any, Dict, List, Optional

from sigfig import round

from hdx.location.currency import Currency
from hdx.location.wfp_api import WFPAPI
from hdx.location.wfp_exchangerates import WFPExchangeRates
from hdx.utilities.dateparse import now_utc, parse_date
from hdx.utilities.loader import load_text, load_yaml
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_text, save_yaml

logger = logging.getLogger(__name__)


def get_now(retriever: Retrieve):
    if retriever.save:
        fixed_now = now_utc()
        datestring = fixed_now.isoformat()
        path = join(retriever.saved_dir, "now.txt")
        save_text(datestring, path)
    elif retriever.use_saved:
        path = join(retriever.saved_dir, "now.txt")
        datestring = load_text(path)
        fixed_now = parse_date(datestring, include_microseconds=True)
    else:
        fixed_now = now_utc()
    return fixed_now


def get_currencies(wfp_api: WFPAPI,
) -> List[Dict]:
    wfp_fx = WFPExchangeRates(wfp_api)
    currencies = wfp_fx.get_currencies_info()
    return sorted(currencies, key=lambda c: c["code"])

def setup_currency(
    now: datetime,
    retriever: Retrieve,
    wfp_api: WFPAPI,
    wfp_rates_folder: Optional[str] = None,
) -> List[Dict]:
    currencies = get_currencies(wfp_api)
    currency_codes = [x["code"] for x in currencies]
    wfp_fx = WFPExchangeRates(wfp_api)
    if wfp_rates_folder:
        filepath = join(wfp_rates_folder, "wfp_rates.yaml")
        if exists(filepath):
            logger.info(f"Loading WFP FX rates from {filepath}")
            wfp_historic_rates = load_yaml(filepath)
        else:
            wfp_historic_rates = wfp_fx.get_historic_rates(currency_codes)
            logger.info(f"Saving WFP FX rates to {filepath}")
            save_yaml(wfp_historic_rates, filepath)
    else:
        wfp_historic_rates = wfp_fx.get_historic_rates(currency_codes)
    Currency.setup(
        retriever=retriever,
        fallback_historic_to_current=True,
        fallback_current_to_static=False,
        fixed_now=now,
        historic_rates_cache=wfp_historic_rates,
        secondary_historic_rates=wfp_historic_rates,
        use_secondary_historic=True,
    )
    return currencies


def round_min_digits(val: Any, nonevalue: Optional[str] = "") -> Optional[str]:
    if val == "" or val is None:
        return nonevalue
    num_str = "%.2f" % val
    count = 0
    for digit in num_str:
        if digit in "123456789":
            count += 1
    if count < 2:
        num_str = round(val, sigfigs=2, type=str, warn=False)
    return num_str.rstrip("0").rstrip(".")
