from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy import delete, select

from .database.dbcommodity import DBCommodity
from .database.dbcountry import DBCountry
from .database.dbmarket import DBMarket
from .database.dbprice import DBPrice
from .utilities import round_min_digits
from hdx.api.configuration import Configuration
from hdx.database import Database
from hdx.utilities.dateparse import (
    default_date,
    default_enddate,
    iso_string_from_datetime,
)
from hdx.utilities.text import number_format


class DBUpdater:
    BATCH_SIZE = 1000

    tables = {
        "DBCountry": DBCountry,
        "DBCommodity": DBCommodity,
        "DBMarket": DBMarket,
    }

    def __init__(self, configuration: Configuration, database: Database):
        self._hxltags = configuration["hxltags"]
        self._database = database
        self._session = database.get_session()

    def update_commodities(self, dbcommodities: List[Dict]):
        self._session.execute(delete(DBCommodity))
        self._database.batch_populate(dbcommodities, DBCommodity)

    def update_tables(
        self,
        countryiso3: str,
        time_period: Dict,
        hdx_url: str,
        dbmarkets: List,
        dbprices: List,
    ) -> None:
        self._session.execute(
            delete(DBCountry).where(DBCountry.countryiso3 == countryiso3)
        )
        dbcountry = DBCountry(
            countryiso3=countryiso3,
            start_date=time_period["startdate"],
            end_date=time_period["enddate"],
            url=hdx_url,
        )
        self._session.add(dbcountry)

        self._session.execute(
            delete(DBMarket).where(DBMarket.countryiso3 == countryiso3)
        )
        self._database.batch_populate(dbmarkets, DBMarket)

        self._session.execute(
            delete(DBPrice).where(DBPrice.countryiso3 == countryiso3)
        )
        if dbprices:
            self._database.batch_populate(dbprices, DBPrice)

    def get_data_from_tables(self) -> Tuple[Dict, datetime, datetime]:
        start_date = default_enddate
        end_date = default_date

        table_data = {
            "DBCountry": {"rows": []},
            "DBCommodity": {"rows": []},
            "DBMarket": {"rows": []},
        }
        for tablename, info in table_data.items():
            dbtable = self.tables[tablename]
            for result in self._session.scalars(select(dbtable)).all():
                row = {}
                for column in result.__table__.columns.keys():
                    value = getattr(result, column)
                    if column == "start_date":
                        if value < start_date:
                            start_date = value
                        value = iso_string_from_datetime(value)
                    elif column == "end_date":
                        if value > end_date:
                            end_date = value
                        value = iso_string_from_datetime(value)
                    elif isinstance(value, float):
                        value = number_format(
                            value, format="%.2f", trailing_zeros=False
                        )
                    row[column] = value
                info["rows"].append(row)
            headers = dbtable.__table__.columns.keys()
            info["headers"] = headers
            info["hxltags"] = {
                header: self._hxltags[header] for header in headers
            }

        columns = [
            DBPrice.countryiso3,
            DBPrice.date,
            DBMarket.admin1,
            DBMarket.admin2,
            DBMarket.market,
            DBMarket.latitude,
            DBMarket.longitude,
            DBCommodity.category,
            DBCommodity.commodity,
            DBPrice.unit,
            DBPrice.priceflag,
            DBPrice.pricetype,
            DBPrice.currency,
            DBPrice.price,
            DBPrice.usdprice,
        ]
        filters = [
            DBPrice.market_id == DBMarket.market_id,
            DBPrice.commodity_id == DBCommodity.commodity_id,
        ]
        order = [
            DBPrice.countryiso3,
            DBPrice.priceflag,
            DBPrice.date,
            DBMarket.admin1,
            DBMarket.admin2,
            DBMarket.market,
            DBCommodity.category,
            DBCommodity.commodity,
            DBPrice.unit,
            DBPrice.pricetype,
        ]
        rows = []
        headers = [col.key for col in columns]
        hxltags = {header: self._hxltags[header] for header in headers}
        table_data["DBPrice"] = {
            "rows": rows,
            "headers": headers,
            "hxltags": hxltags,
        }
        for result in self._session.execute(
            select(*columns).where(*filters).order_by(*order)
        ):
            row = {}
            for i, column in enumerate(columns):
                value = result[i]
                if column.key == "usdprice":
                    value = round_min_digits(value, None)
                elif isinstance(value, float):
                    value = number_format(
                        value, format="%.2f", trailing_zeros=False
                    )
                elif isinstance(value, datetime):
                    value = iso_string_from_datetime(value)
                row[column.key] = value
            rows.append(row)
        return table_data, start_date, end_date
