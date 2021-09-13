from hdx.database import Base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declared_attr


class DBCommodity(Base):
    @declared_attr
    def __tablename__(cls):
        return "dbcommodities"

    commodity_id = Column(Integer, primary_key=True)
    category = Column(String, nullable=False)
    commodity = Column(String, nullable=False)

    def __repr__(self):
        output = f"<Commodity(id={self.commodity_id}, category={self.category}, "
        output += f"name={self.commodity})>"
        return output
