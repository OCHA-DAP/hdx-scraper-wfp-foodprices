# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, String, Float, Integer, ForeignKey

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
        output = '<Market(id=%d, name=%s, ' % (self.market_id, self.market)
        output += 'admin1=%s, admin2=%s,\n' % (self.admin1, self.admin2)
        output += 'latitude=%s, longitude=%s)>' % (str(self.latitude), str(self.longitude))
        return output
