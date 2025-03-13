#!/usr/bin/python
"""
WFP food prices:
------------

Creates datasets with flattened tables of WFP food prices.

"""

import logging
from os import getenv
from typing import Dict, List

from sqlalchemy import delete
from sqlalchemy.orm import Session

from database.dbcommodity import DBCommodity

from hdx.api.configuration import Configuration
from hdx.location.wfp_api import WFPAPI
from hdx.utilities.retriever import Retrieve

logger = logging.getLogger(__name__)


class WFPMappings:
    def __init__(
        self,
        configuration: Configuration,
        wfp_api: WFPAPI,
        retriever: Retrieve,
        session: Session,
    ):
        self.configuration = configuration
        self.wfp_api = wfp_api
        self.retriever = retriever
        self.session = session

    def read_region_mapping(self) -> Dict[str, str]:
        headers, rows = self.retriever.get_tabular_rows(
            self.configuration["region_mapping_url"],
            dict_form=True,
            filename="region_mapping.csv",
        )
        iso3_to_showcase_url = {}
        for row in rows:
            countryiso3 = row["iso3"]
            name = row["name"]
            region = row["region"]
            url = f"https://dataviz.vam.wfp.org/{region}/{name}/overview"
            iso3_to_showcase_url[countryiso3] = url
        return iso3_to_showcase_url

    def read_source_overrides(self) -> Dict[str, str]:
        headers, rows = self.retriever.get_tabular_rows(
            self.configuration["source_overrides_url"],
            dict_form=True,
            filename="source_overrides.csv",
        )
        iso3_to_source = {}
        for row in rows:
            countryiso3 = row["Iso3"]
            source = row["Source override"]
            iso3_to_source[countryiso3] = source
        return iso3_to_source

    def get_countries(self) -> List[Dict[str, str]]:
        url = self.configuration["countries_url"]
        json = self.wfp_api.retrieve(url, "countries.json", "countries")
        countries = set()
        for country in json["response"]:
            countryiso3 = country["iso3"]
            if self.retriever.save:
                if countryiso3 not in (
                    "BLR",
                    "COG",
                    "PSE",
                    "SYR",
                ):
                    continue
                wheretostart = getenv("WHERETOSTART")
                if wheretostart and countryiso3 not in wheretostart:
                    continue
            countries.add((country["iso3"], country["adm0_name"]))
        return [{"iso3": x[0], "name": x[1]} for x in sorted(countries)]

    def build_commodity_category_mapping(self) -> Dict[str, str]:
        self.session.execute(delete(DBCommodity))
        categoryid_to_name = {}
        for category in self.wfp_api.get_items("Commodities/Categories/List"):
            categoryid_to_name[category["id"]] = category["name"]
        commodity_to_category = {}
        for commodity in self.wfp_api.get_items("Commodities/List"):
            commodity_id = commodity["id"]
            commodity_name = commodity["name"]
            category = categoryid_to_name[commodity["categoryId"]]
            commodity_to_category[commodity_id] = categoryid_to_name[
                commodity["categoryId"]
            ]
            dbcommodity = DBCommodity(
                commodity_id=commodity_id,
                category=category,
                commodity=commodity_name,
            )
            self.session.add(dbcommodity)
        self.session.commit()
        return commodity_to_category
