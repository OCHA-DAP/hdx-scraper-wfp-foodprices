from os.path import join

from hdx.location.currency import Currency
from hdx.location.wfp_api import WFPAPI
from hdx.location.wfp_exchangerates import WFPExchangeRates
from hdx.utilities.dateparse import now_utc, parse_date
from hdx.utilities.loader import load_text
from hdx.utilities.retriever import Retrieve
from hdx.utilities.saver import save_text


def setup_currency(retriever: Retrieve, wfp_api: WFPAPI) -> None:
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
        fixed_now = None
    wfp_fx = WFPExchangeRates(wfp_api)
    currencies = wfp_fx.get_currencies()
    all_historic_rates = wfp_fx.get_historic_rates(currencies)
    Currency.setup(
        retriever=retriever,
        fallback_historic_to_current=True,
        fallback_current_to_static=False,
        fixed_now=fixed_now,
        historic_rates_cache=all_historic_rates,
    )
