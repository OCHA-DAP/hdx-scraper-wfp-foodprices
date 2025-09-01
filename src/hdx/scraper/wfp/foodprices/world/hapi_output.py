import logging
from copy import deepcopy
from os.path import join
from typing import Dict, List, Optional

from dateutil.relativedelta import relativedelta

from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.dictandlist import write_list_to_csv
from hdx.utilities.downloader import Download
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class HAPIOutput:
    def __init__(
        self,
        configuration: Configuration,
        downloader: Download,
        folder: str,
        error_handler: HDXErrorHandler,
    ) -> None:
        self._configuration = configuration
        self._downloader = downloader
        self._folder = folder
        self._error_handler = error_handler
        self._admins = []
        self._base_rows = {}

    def setup_admins(
        self,
        retriever: Retrieve,
        countryiso3s: Optional[List[str]] = None,
    ):
        libhxl_dataset = AdminLevel.get_libhxl_dataset(retriever=retriever).cache()
        libhxl_format_dataset = AdminLevel.get_libhxl_dataset(
            url=AdminLevel.formats_url, retriever=retriever
        ).cache()
        self._admins = []
        for i in range(2):
            admin = AdminLevel(admin_level=i + 1, retriever=retriever)
            admin.setup_from_libhxl_dataset(
                libhxl_dataset=libhxl_dataset,
                countryiso3s=countryiso3s,
            )
            admin.load_pcode_formats_from_libhxl_dataset(libhxl_format_dataset)
            self._admins.append(admin)

    def complete_admin(self, row: Dict, base_row: Dict):
        market_name = row["market"]
        base_row["market_name"] = market_name
        base_row["market_code"] = row["market_id"]
        countryiso3 = row["countryiso3"]
        base_row["location_code"] = countryiso3
        base_row["has_hrp"] = (
            "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
        )
        base_row["in_gho"] = (
            "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
        )
        base_row["lat"] = row["latitude"] or ""
        base_row["lon"] = row["longitude"] or ""
        provider_admin1_name = row["admin1"]
        provider_admin2_name = row["admin2"]
        base_row["provider_admin1_name"] = provider_admin1_name or ""
        base_row["provider_admin2_name"] = provider_admin2_name or ""
        if countryiso3 in self._configuration["unused_adm1"]:
            if provider_admin2_name:
                base_row["admin_level"] = 1
                adm1_code, _ = self._admins[0].get_pcode(
                    countryiso3, provider_admin2_name
                )
                if adm1_code:
                    base_row["admin1_code"] = adm1_code
                    base_row["admin1_name"] = self._admins[0].pcode_to_name[adm1_code]
            else:
                base_row["admin_level"] = 0
                self._error_handler.add_missing_value_message(
                    "WFPFoodPrice",
                    countryiso3,
                    "admin 1 name for market",
                    market_name,
                    message_type="warning",
                )
                base_row["warning"].add("no adm1 name in prov2 name")
            return

        if countryiso3 in self._configuration["unused_adm2"]:
            if provider_admin1_name:
                base_row["admin_level"] = 2
                adm2_code, _ = self._admins[1].get_pcode(
                    countryiso3, provider_admin1_name
                )
                if adm2_code:
                    base_row["admin2_code"] = adm2_code
                    base_row["admin2_name"] = self._admins[1].pcode_to_name[adm2_code]
                    adm1_code = self._admins[1].pcode_to_parent.get(adm2_code)
                    if adm1_code:
                        base_row["admin1_code"] = adm1_code
                        base_row["admin2_name"] = self._admins[0].pcode_to_name[
                            adm1_code
                        ]
            else:
                base_row["admin_level"] = 0
                self._error_handler.add_missing_value_message(
                    "WFPFoodPrice",
                    countryiso3,
                    "admin 2 name for market",
                    market_name,
                    message_type="warning",
                )
                base_row["warning"].add("no adm2 name in prov1 name")
            return

        if provider_admin1_name:
            base_row["admin_level"] = 1
            adm1_code, _ = self._admins[0].get_pcode(countryiso3, provider_admin1_name)
            if adm1_code:
                base_row["admin1_code"] = adm1_code
                base_row["admin1_name"] = self._admins[0].pcode_to_name[adm1_code]
        else:
            adm1_code = ""
            base_row["admin_level"] = 0
            self._error_handler.add_missing_value_message(
                "WFPFoodPrice",
                countryiso3,
                "admin 1 name for market",
                market_name,
                message_type="warning",
            )
            base_row["warning"].add("no adm1 name")

        if countryiso3 in self._configuration["adm1_only"]:
            return

        if provider_admin2_name:
            base_row["admin_level"] = 2
            adm2_code, _ = self._admins[1].get_pcode(
                countryiso3, provider_admin2_name, parent=adm1_code
            )
            if adm2_code:
                base_row["admin2_code"] = adm2_code
                base_row["admin2_name"] = self._admins[1].pcode_to_name[adm2_code]
                parent_code = self._admins[1].pcode_to_parent.get(adm2_code)
                if adm1_code and adm1_code != parent_code:
                    message = f"PCode mismatch {adm1_code}->{parent_code} (parent)"
                    self._error_handler.add_message(
                        "WFPFoodPrice",
                        f"{countryiso3}-{adm2_code}",
                        message,
                        market_name,
                        message_type="warning",
                    )
                    base_row["warning"].add(message)
                    base_row["admin1_code"] = parent_code
                    base_row["admin1_name"] = self._admins[0].pcode_to_name[parent_code]
            return

        if adm1_code:
            identifier = f"{countryiso3}-{adm1_code}"
        elif provider_admin1_name:
            identifier = f"{countryiso3}-{provider_admin1_name}"
        else:
            identifier = countryiso3
        self._error_handler.add_missing_value_message(
            "WFPFoodPrice",
            identifier,
            "admin 2 name for market",
            market_name,
            message_type="warning",
        )
        base_row["warning"].add("no adm2 name")

    def process_currencies(
        self, currencies: List[Dict], dataset_id: str, resource_id: str
    ) -> List[Dict]:
        logger.info("Processing HAPI currencies output")
        for row in currencies:
            row["dataset_hdx_id"] = dataset_id
            row["resource_hdx_id"] = resource_id
        return currencies

    def process_commodities(
        self, commodities: List[Dict], dataset_id: str, resource_id: str
    ) -> List[Dict]:
        logger.info("Processing HAPI commodities output")
        hapi_rows = []
        for row in commodities:
            hapi_row = {
                "code": row["commodity_id"],
                "category": row["category"],
                "name": row["commodity"],
                "dataset_hdx_id": dataset_id,
                "resource_hdx_id": resource_id,
            }
            hapi_rows.append(hapi_row)
        hapi_rows = sorted(
            hapi_rows, key=lambda row: (row["category"], row["name"], row["code"])
        )
        return hapi_rows

    def process_markets(
        self, markets: List[Dict], dataset_id: str, resource_id: str
    ) -> List[Dict]:
        logger.info("Processing HAPI markets output")
        hapi_rows = []
        for row in markets:
            hapi_base_row = {
                "admin1_code": "",
                "admin1_name": "",
                "admin2_code": "",
                "admin2_name": "",
                "warning": set(),
                "error": set(),
            }
            self.complete_admin(row, hapi_base_row)
            self._base_rows[hapi_base_row["market_code"]] = hapi_base_row
            hapi_row = deepcopy(hapi_base_row)
            hapi_row["dataset_hdx_id"] = dataset_id
            hapi_row["resource_hdx_id"] = resource_id
            hapi_row["warning"] = "|".join(sorted(hapi_row["warning"]))
            hapi_row["error"] = "|".join(sorted(hapi_row["error"]))
            hapi_rows.append(hapi_row)
        hapi_rows = sorted(
            hapi_rows,
            key=lambda row: (
                row["location_code"],
                row["admin1_code"],
                row["admin2_code"],
                row["provider_admin1_name"],
                row["provider_admin2_name"],
                row["market_name"],
                row["market_code"],
            ),
        )
        return hapi_rows

    def create_prices_files(
        self,
        year_to_path: Dict,
        dataset_id: str,
        year_to_prices_resource_id: Dict,
        output_dir: str = "",
    ) -> Dict:
        logger.info("Processing HAPI prices output")
        configuration = self._configuration["hapi_dataset"]["resources"][0]
        hxltags = configuration["hxltags"]
        headers = list(hxltags.keys())

        hapi_year_to_path = {}
        years = sorted(year_to_path.keys(), reverse=True)
        for year in years[:10]:
            rows = [hxltags]
            filepath = year_to_path[year]
            _, iterator = self._downloader.get_tabular_rows(
                filepath, has_hxl=True, dict_form=True, encoding="utf-8"
            )
            logger.info(f"Reading global prices from {filepath}")
            for row in iterator:
                market_id = row["market_id"]
                if market_id[0] == "#":
                    continue
                hapi_row = deepcopy(self._base_rows[market_id])
                hapi_row["dataset_hdx_id"] = dataset_id
                hapi_row["resource_hdx_id"] = year_to_prices_resource_id[year]
                hapi_row["commodity_category"] = row["category"]
                hapi_row["commodity_name"] = row["commodity"]
                hapi_row["commodity_code"] = row["commodity_id"]
                hapi_row["unit"] = row["unit"]
                hapi_row["price_flag"] = row["priceflag"]
                hapi_row["price_type"] = row["pricetype"]
                hapi_row["currency_code"] = row["currency"]
                hapi_row["price"] = row["price"]
                hapi_row["usd_price"] = row["usdprice"]
                reference_period_start = parse_date(row["date"], date_format="%Y-%m-%d")
                hapi_row["reference_period_start"] = iso_string_from_datetime(
                    reference_period_start
                )
                reference_period_end = reference_period_start + relativedelta(
                    months=1,
                    days=-1,
                    hours=23,
                    minutes=59,
                    seconds=59,
                    microseconds=999999,
                )  # food price reference period is one month
                hapi_row["reference_period_end"] = iso_string_from_datetime(
                    reference_period_end
                )
                hapi_row["warning"] = "|".join(sorted(hapi_row["warning"]))
                hapi_row["error"] = "|".join(sorted(hapi_row["error"]))
                rows.append(hapi_row)
            if not output_dir:
                output_dir = self._folder
            filename = configuration["filename"].format(year)
            filepath = join(output_dir, filename)
            write_list_to_csv(filepath, rows, columns=headers)
            hapi_year_to_path[year] = filepath

        return hapi_year_to_path
