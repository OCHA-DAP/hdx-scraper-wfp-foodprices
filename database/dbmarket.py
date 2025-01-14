from sqlalchemy.orm import Mapped, mapped_column

from database.dbcountry import DBCountry
from hdx.database import NoTZBase
from sqlalchemy import ForeignKey


class DBMarket(NoTZBase):
    market_id: Mapped[int] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(nullable=False)
    countryiso3: Mapped[str] = mapped_column(ForeignKey(DBCountry.countryiso3))
    admin1: Mapped[str] = mapped_column(nullable=True)
    admin2: Mapped[str] = mapped_column(nullable=True)
    latitude: Mapped[float] = mapped_column(nullable=True)
    longitude: Mapped[float] = mapped_column(nullable=True)

    def __repr__(self):
        output = f"<Market(id={self.market_id}, name={self.market}, "
        output += f"admin1={self.admin1}, admin2={self.admin2},\n"
        output += f"latitude={str(self.latitude)}, longitude={str(self.longitude)})>"
        return output
