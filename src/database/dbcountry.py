# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, String, DateTime
from sqlalchemy.orm import declared_attr


class DBCountry(Base):
    @declared_attr
    def __tablename__(cls):
        return 'dbcountries'

    countryiso3 = Column(String, primary_key=True)
    url = Column(String, nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)

    def __repr__(self):
        output = '<Countries(country=%s, url=%d,\n' % (self.countryiso3, self.url)
        output += 'startdate=%s, enddate=%s)>' % (str(self.start_date), str(self.end_date))
        return output
