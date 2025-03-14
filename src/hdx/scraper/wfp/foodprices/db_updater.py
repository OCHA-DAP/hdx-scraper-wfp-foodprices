from datetime import datetime
from typing import Dict, List, Tuple

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from .database.dbcommodity import DBCommodity
from .database.dbcountry import DBCountry
from .database.dbmarket import DBMarket
from hdx.api.configuration import Configuration
from hdx.utilities.dateparse import default_date, default_enddate


class DBUpdater:
    tables = {
        "DBCountry": DBCountry,
        "DBCommodity": DBCommodity,
        "DBMarket": DBMarket,
    }

    def __init__(self, configuration: Configuration, session: Session):
        self._hxltags = configuration["hxltags"]
        self._session = session

    def update_tables(
        self,
        countryiso3: str,
        dbmarkets: List[DBMarket],
        time_period: Dict,
        hdx_url: str,
    ) -> None:
        self._session.execute(
            delete(DBCountry).where(DBCountry.countryiso3 == countryiso3)
        )
        self._session.execute(
            delete(DBMarket).where(DBMarket.countryiso3 == countryiso3)
        )
        dbcountry = DBCountry(
            countryiso3=countryiso3,
            start_date=time_period["startdate"],
            end_date=time_period["enddate"],
            url=hdx_url,
        )
        self._session.add(dbcountry)
        for dbmarket in dbmarkets:
            self._session.add(dbmarket)
        self._session.commit()

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
                    row[column] = getattr(result, column)
                    if column == "start_date":
                        if row[column] < start_date:
                            start_date = row[column]
                    elif column == "end_date":
                        if row[column] > end_date:
                            end_date = row[column]
                info["rows"].append(row)
            headers = dbtable.__table__.columns.keys()
            info["headers"] = headers
            info["hxltags"] = {
                header: self._hxltags[header] for header in headers
            }
        return table_data, start_date, end_date
