from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from hdx.database.no_timezone import Base
from hdx.scraper.wfp.foodprices.database.dbcommodity import DBCommodity
from hdx.scraper.wfp.foodprices.database.dbcountry import DBCountry
from hdx.scraper.wfp.foodprices.database.dbmarket import DBMarket


class DBPrice(Base):
    countryiso3: Mapped[str] = mapped_column(ForeignKey(DBCountry.countryiso3))
    date: Mapped[datetime] = mapped_column(primary_key=True)
    market_id: Mapped[int] = mapped_column(
        ForeignKey(DBMarket.market_id), primary_key=True
    )
    commodity_id: Mapped[int] = mapped_column(
        ForeignKey(DBCommodity.commodity_id), primary_key=True
    )
    unit: Mapped[str] = mapped_column(primary_key=True)
    priceflag: Mapped[str] = mapped_column(primary_key=True)
    pricetype: Mapped[str] = mapped_column(primary_key=True)
    currency: Mapped[str] = mapped_column(nullable=False)
    price: Mapped[float] = mapped_column(nullable=False)
    usdprice: Mapped[float] = mapped_column(nullable=False)

    def __repr__(self):
        output = f"<Price(iso3={self.countryiso3}, date={str(self.date)}, market_id={self.market_id}, commodity_id={self.commodity_id},\n"
        output += f"unit={self.unit}, priceflag={self.priceflag}, pricetype={self.pricetype},\n)>"
        output += f"currency={self.currency}, price={str(self.price)}, usdprice={str(self.usdprice)},\n)>"
        return output
