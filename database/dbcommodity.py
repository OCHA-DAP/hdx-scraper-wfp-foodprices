# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import declared_attr


class DBCommodity(Base):
    @declared_attr
    def __tablename__(cls):
        return 'dbcommodities'

    commodity_id = Column(Integer, primary_key=True)
    category = Column(String, nullable=False)
    commodity = Column(String, nullable=False)

    def __repr__(self):
        output = '<Commodity(id=%d, category=%s, ' % (self.commodity_id, self.category)
        output += 'name=%s)>' % self.commodity
        return output
