from sqlalchemy.orm import Mapped, mapped_column

from hdx.database.no_timezone import Base


class DBCommodity(Base):
    commodity_id: Mapped[int] = mapped_column(primary_key=True)
    category: Mapped[str] = mapped_column(nullable=False)
    commodity: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self):
        output = (
            f"<Commodity(id={self.commodity_id}, category={self.category}, "
        )
        output += f"name={self.commodity})>"
        return output
