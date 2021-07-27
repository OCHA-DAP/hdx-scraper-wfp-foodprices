# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, DateTime, String, Float, Integer, Boolean, ForeignKey

from database.dbcommodity import DBCommodity
from database.dbmarket import DBMarket


class DBFoodPrice(Base):
    date = Column(DateTime, primary_key=True)
    countryiso3 = Column(String, nullable=False)
    market_id = Column(Integer, ForeignKey(DBMarket.id), primary_key=True)
    commodity_id = Column(Integer, ForeignKey(DBCommodity.id), primary_key=True)
    unit = Column(String, primary_key=True)
    pricetype = Column(Boolean, primary_key=True)
    currency = Column(String, primary_key=True)
    price = Column(Float, nullable=False)
    usdprice = Column(Float)

    def __repr__(self):
        output = '<FoodPrice(date=%s, country=%s, market=%d' % (str(self.date), self.countryiso3, self.market)
        output += 'commmodity=%d, unit=%s\n' % (self.commodity, self.unit)
        output += 'pricetype=%s, currency=%s, ' % (self.pricetype, self.currency)
        output += 'price=%f, usdprice=%f)>' % (self.price, self.usdprice)
        return output
