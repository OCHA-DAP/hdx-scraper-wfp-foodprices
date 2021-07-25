# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, DateTime, String, Float


class DBFoodPrice(Base):
    countryiso3 = Column(String, primary_key=True)
    date = Column(DateTime, primary_key=True)
    adm1name = Column(String, primary_key=True)
    adm2name = Column(String, primary_key=True)
    market = Column(String, primary_key=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    category = Column(String, primary_key=True)
    commodity = Column(String, primary_key=True)
    unit = Column(String, primary_key=True)
    pricetype = Column(String, primary_key=True)
    currency = Column(String, primary_key=True)
    price = Column(Float, nullable=False)
    usdprice = Column(Float, nullable=False)

    def __repr__(self):
        output = '<FoodPrice(countryiso3=%s, date=%s, ' % (str(self.date), self.countryiso3)
        output += 'adm1name=%s, adm2name=%s,\n' % (str(self.dataset_date), self.update_frequency)
        output += 'market=%s, latitude=%f, longitude=%f, ' % (self.market, self.latitude, self.longitude)
        output += 'category=%s, commodity=%s,\n' % (self.category, self.commodity)
        output += 'unit=%s, pricetype=%s, currency=%s, ' % (self.unit, self.pricetype, self.currency)
        output += 'price=%f, usdprice=%f)>' % (self.price, self.usdprice)
        return output
