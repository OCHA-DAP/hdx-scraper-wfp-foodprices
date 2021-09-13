from hdx.database import Base
from sqlalchemy import Column, Float, ForeignKey, Integer, String

from database.dbcountry import DBCountry


class DBMarket(Base):
    market_id = Column(Integer, primary_key=True)
    market = Column(String, nullable=False)
    countryiso3 = Column(String, ForeignKey(DBCountry.countryiso3))
    admin1 = Column(String)
    admin2 = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    def __repr__(self):
        output = f"<Market(id={self.market_id}, name={self.market}, "
        output += f"admin1={self.admin1}, admin2={self.admin2},\n"
        output += f"latitude={str(self.latitude)}, longitude={str(self.longitude)})>"
        return output
