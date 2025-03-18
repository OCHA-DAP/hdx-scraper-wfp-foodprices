from datetime import datetime
from os.path import join

from sigfig import round

from hdx.location.currency import Currency
from hdx.location.wfp_api import WFPAPI
from hdx.location.wfp_exchangerates import WFPExchangeRates
from hdx.utilities.dateparse import now_utc, parse_date
from hdx.utilities.loader import load_text
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_text


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


def setup_currency(
    now: datetime, retriever: Retrieve, wfp_api: WFPAPI
) -> None:
    wfp_fx = WFPExchangeRates(wfp_api)
    currencies = wfp_fx.get_currencies()
    all_historic_rates = wfp_fx.get_historic_rates(currencies)
    Currency.setup(
        retriever=retriever,
        fallback_historic_to_current=True,
        fallback_current_to_static=False,
        fixed_now=now,
        historic_rates_cache=all_historic_rates,
    )


def round_min_digits(num: float) -> str:
    num_str = "%.2f" % num
    count = 0
    for digit in num_str:
        if digit in "123456789":
            count += 1
    if count < 2:
        num_str = round(num, sigfigs=2, type=str, warn=False)
    return num_str.rstrip("0").rstrip(".")
