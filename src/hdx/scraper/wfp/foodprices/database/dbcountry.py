from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column

from hdx.database.no_timezone import Base


class DBCountry(Base):
    countryiso3: Mapped[str] = mapped_column(primary_key=True)
    url: Mapped[str] = mapped_column(nullable=False)
    start_date: Mapped[datetime] = mapped_column(nullable=False)
    end_date: Mapped[datetime] = mapped_column(nullable=False)

    def __repr__(self):
        output = f"<Country(country={self.countryiso3}, url={self.url},\n"
        output += f"startdate={str(self.start_date)}, enddate={str(self.end_date)})>"
        return output
