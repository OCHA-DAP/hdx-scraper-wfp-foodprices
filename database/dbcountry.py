from hdx.database import Base
from sqlalchemy import Column, String
from sqlalchemy.orm import declared_attr
from sqlalchemy_utc import UtcDateTime


class DBCountry(Base):
    @declared_attr
    def __tablename__(cls):
        return "dbcountries"

    countryiso3 = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    start_date = Column(UtcDateTime(), nullable=False)
    end_date = Column(UtcDateTime(), nullable=False)

    def __repr__(self):
        output = f"<Country(country={self.countryiso3}, url={self.url},\n"
        output += f"startdate={str(self.start_date)}, enddate={str(self.end_date)})>"
        return output
