# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, String, Float, Integer


class DBMarket(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    countryiso3 = Column(String, nullable=False)
    admin1 = Column(String)
    admin2 = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)

    def __repr__(self):
        output = '<Market(id=%d, name=%s, ' % (self.id, self.name)
        output += 'admin1=%s, admin2=%s,\n' % (self.admin1, self.admin2)
        output += 'latitude=%s, longitude=%s)>' % (str(self.latitude), str(self.longitude))
        return output
