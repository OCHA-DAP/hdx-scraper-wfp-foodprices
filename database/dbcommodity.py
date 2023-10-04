from hdx.database import NoTZBase
from sqlalchemy.orm import declared_attr, Mapped, mapped_column


class DBCommodity(NoTZBase):
    commodity_id: Mapped[int] = mapped_column(primary_key=True)
    category: Mapped[str] = mapped_column(nullable=False)
    commodity: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self):
        output = f"<Commodity(id={self.commodity_id}, category={self.category}, "
        output += f"name={self.commodity})>"
        return output
