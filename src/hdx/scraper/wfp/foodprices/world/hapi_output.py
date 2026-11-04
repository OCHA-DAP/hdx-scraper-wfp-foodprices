import csv
import logging
from copy import deepcopy
from functools import cache
from os.path import join

from dateutil.relativedelta import relativedelta
from hdx.api.configuration import Configuration
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.location.adminlevel import AdminLevel
from hdx.location.country import Country
from hdx.utilities.dateparse import iso_string_from_datetime, parse_date
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


@cache
def get_reference_period(date: str) -> tuple[str, str]:
    reference_period_start = parse_date(date, date_format="%Y-%m-%d")
    reference_period_end = reference_period_start + relativedelta(
        months=1,
        days=-1,
        hours=23,
        minutes=59,
        seconds=59,
        microseconds=999999,
    )  # food price reference period is one month
    return iso_string_from_datetime(reference_period_start), iso_string_from_datetime(
        reference_period_end
    )


class HAPIOutput:
    def __init__(
        self,
        configuration: Configuration,
        folder: str,
        error_handler: HDXErrorHandler,
    ) -> None:
        self._configuration = configuration
        self._folder = folder
        self._error_handler = error_handler
        self._admins = []
        self._base_rows = {}

    def setup_admins(
        self,
        retriever: Retrieve,
        countryiso3s: list[str] | None = None,
    ):
        _, iterator = retriever.get_tabular_rows(AdminLevel.admin_url, dict_form=True)
        pcode_rows = []
        for row in iterator:
            if countryiso3s and row["Location"] not in countryiso3s:
                continue
            pcode_rows.append(row)
        _, iterator = retriever.get_tabular_rows(AdminLevel.formats_url, dict_form=True)
        pcode_formats_rows = []
        for row in iterator:
            if countryiso3s and row["Location"] not in countryiso3s:
                continue
            pcode_formats_rows.append(row)
        self._admins = []
        for i in range(2):
            admin_level = i + 1
            admin = AdminLevel(
                admin_config=self._configuration.get(f"admin{admin_level}_config"),
                admin_level=admin_level,
                retriever=retriever,
            )
            admin.setup_from_iterable(pcode_rows)
            admin.load_pcode_formats_from_iterable(pcode_formats_rows)
            self._admins.append(admin)
        self._admins[1].set_parent_admins_from_adminlevels([self._admins[0]])

    def complete_admin(self, row: dict, base_row: dict):
        market_name = row["market"]
        countryiso3 = row["countryiso3"]
        provider_admin1_name = row["admin1"]
        provider_admin2_name = row["admin2"]
        base_row["provider_admin1_name"] = provider_admin1_name or ""
        base_row["provider_admin2_name"] = provider_admin2_name or ""
        base_row["admin1_code"] = ""
        base_row["admin1_name"] = ""
        base_row["admin2_code"] = ""
        base_row["admin2_name"] = ""
        if countryiso3 in self._configuration["unused_adm1"]:
            if provider_admin2_name:
                adm1_code, _ = self._admins[0].get_pcode(
                    countryiso3, provider_admin2_name
                )
                if adm1_code:
                    base_row["admin1_code"] = adm1_code
                    base_row["admin1_name"] = self._admins[0].pcode_to_name[adm1_code]
                base_row["admin_level"] = 1
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
                adm2_code, _ = self._admins[1].get_pcode(
                    countryiso3, provider_admin1_name
                )
                if adm2_code:
                    base_row["admin2_code"] = adm2_code
                    base_row["admin2_name"] = self._admins[1].pcode_to_name[adm2_code]
                    adm1_code = self._admins[1].pcode_to_parent.get(adm2_code)
                    if adm1_code:
                        base_row["admin1_code"] = adm1_code
                        base_row["admin1_name"] = self._admins[0].pcode_to_name[
                            adm1_code
                        ]
                base_row["admin_level"] = 2
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
            adm1_code, _ = self._admins[0].get_pcode(countryiso3, provider_admin1_name)
            if adm1_code:
                base_row["admin1_code"] = adm1_code
                base_row["admin1_name"] = self._admins[0].pcode_to_name[adm1_code]
            base_row["admin_level"] = 1
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
                elif not adm1_code and parent_code:
                    base_row["admin1_code"] = parent_code
                    base_row["admin1_name"] = self._admins[0].pcode_to_name[parent_code]
            base_row["admin_level"] = 2
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

    def complete_base_row(self, row: dict, base_row: dict):
        countryiso3 = row["countryiso3"]
        base_row["location_code"] = countryiso3
        base_row["has_hrp"] = (
            "Y" if Country.get_hrp_status_from_iso3(countryiso3) else "N"
        )
        base_row["in_gho"] = (
            "Y" if Country.get_gho_status_from_iso3(countryiso3) else "N"
        )
        self.complete_admin(row, base_row)
        base_row["market_name"] = row["market"]
        base_row["market_code"] = row["market_id"]
        base_row["lat"] = row["latitude"] or ""
        base_row["lon"] = row["longitude"] or ""

    def process_commodities(self, commodities: list[dict]) -> list[dict]:
        logger.info("Processing HAPI commodities output")
        hapi_rows = []
        for row in commodities:
            hapi_row = {
                "category": row["category"],
                "name": row["commodity"],
                "code": row["commodity_id"],
            }
            hapi_rows.append(hapi_row)
        hapi_rows = sorted(
            hapi_rows, key=lambda row: (row["category"], row["name"], row["code"])
        )
        return hapi_rows

    @classmethod
    def add_warnings_errors(cls, hapi_row: dict):
        warnings = "|".join(sorted(hapi_row["warning"]))
        del hapi_row["warning"]
        hapi_row["warning"] = warnings
        errors = "|".join(sorted(hapi_row["error"]))
        del hapi_row["error"]
        hapi_row["error"] = errors

    def process_markets(
        self, markets: list[dict], dataset_id: str, resource_id: str
    ) -> list[dict]:
        logger.info("Processing HAPI markets output")
        hapi_rows = []
        for row in markets:
            hapi_base_row = {
                "warning": set(),
                "error": set(),
            }
            self.complete_base_row(row, hapi_base_row)
            self._base_rows[hapi_base_row["market_code"]] = hapi_base_row
            hapi_row = deepcopy(hapi_base_row)
            hapi_row["dataset_hdx_id"] = dataset_id
            hapi_row["resource_hdx_id"] = resource_id
            self.add_warnings_errors(hapi_row)
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
        year_to_path: dict,
        dataset_id: str,
        year_to_prices_resource_id: dict,
        output_dir: str = "",
    ) -> dict:
        logger.info("Processing HAPI prices output")
        configuration = self._configuration["hapi_dataset"]["resources"][0]
        headers = configuration["headers"]

        market_id_to_row = {}
        for market_id, base_row in self._base_rows.items():
            market_row = deepcopy(base_row)
            self.add_warnings_errors(market_row)
            market_id_to_row[market_id] = market_row

        if not output_dir:
            output_dir = self._folder
        hapi_year_to_path = {}
        years = sorted(year_to_path.keys(), reverse=True)
        for year in years[:10]:
            filepath = year_to_path[year]
            logger.info(f"Reading global prices from {filepath}")
            resource_id = year_to_prices_resource_id[year]
            hapi_filepath = join(output_dir, configuration["filename"].format(year))
            with (
                open(filepath, encoding="utf-8", newline="") as input_file,
                open(hapi_filepath, "w", encoding="utf-8", newline="") as output_file,
            ):
                writer = csv.DictWriter(output_file, headers, extrasaction="ignore")
                writer.writeheader()
                for row in csv.DictReader(input_file):
                    hapi_row = market_id_to_row[row["market_id"]].copy()
                    hapi_row["commodity_category"] = row["category"]
                    hapi_row["commodity_name"] = row["commodity"]
                    hapi_row["commodity_code"] = row["commodity_id"]
                    hapi_row["unit"] = row["unit"]
                    hapi_row["price_flag"] = row["priceflag"]
                    hapi_row["price_type"] = row["pricetype"]
                    hapi_row["currency_code"] = row["currency"]
                    hapi_row["price"] = row["price"]
                    hapi_row["usd_price"] = row["usdprice"]
                    (
                        hapi_row["reference_period_start"],
                        hapi_row["reference_period_end"],
                    ) = get_reference_period(row["date"])
                    hapi_row["dataset_hdx_id"] = dataset_id
                    hapi_row["resource_hdx_id"] = resource_id
                    writer.writerow(hapi_row)
            hapi_year_to_path[year] = hapi_filepath

        return hapi_year_to_path
