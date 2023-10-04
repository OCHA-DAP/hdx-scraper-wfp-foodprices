from datetime import datetime

from hdx.database import NoTZBase
from sqlalchemy.orm import Mapped, mapped_column


class DBCountry(NoTZBase):
    countryiso3: Mapped[str] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(nullable=False)
    start_date: Mapped[datetime] = mapped_column(nullable=False)
    end_date: Mapped[datetime] = mapped_column(nullable=False)

    def __repr__(self):
        output = f"<Country(country={self.countryiso3}, url={self.url},\n"
        output += f"startdate={str(self.start_date)}, enddate={str(self.end_date)})>"
        return output
