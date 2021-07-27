# -*- coding: utf-8 -*-
from hdx.database import Base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import declared_attr


class DBCommodity(Base):
    @declared_attr
    def __tablename__(cls):
        return 'dbcommodities'

    id = Column(Integer, primary_key=True)
    category = Column(String, nullable=False)
    name = Column(String, nullable=False)

    def __repr__(self):
        output = '<Category(id=%d, category=%s, ' % (self.id, self.category)
        output += 'name=%s)>' % self.name
        return output
