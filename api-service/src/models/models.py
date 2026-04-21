from datetime import date
from sqlalchemy import BigInteger, ForeignKey, Integer, String, SmallInteger, Date
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseORM(DeclarativeBase):
    __table_args__ = {"schema": "dwh"}


class CalendarORM(BaseORM):
    __tablename__ = "d_calendar"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fact_date: Mapped[date] = mapped_column(Date, nullable=False)
    week_of_year: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    month_name: Mapped[str] = mapped_column(String(9), nullable=False)


class CountryORM(BaseORM):
    __tablename__ = "d_country"

    id: Mapped[int] = mapped_column(SmallInteger, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)


class CityORM(BaseORM):
    __tablename__ = "d_city"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    country_id: Mapped[int] = mapped_column(ForeignKey("dwh.d_country.id"), nullable=False)


class UserORM(BaseORM):
    __tablename__ = "d_user"

    tg_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    sex: Mapped[str] = mapped_column(String(7), nullable=False)
    city_id: Mapped[int] = mapped_column(ForeignKey("dwh.d_city.id"), nullable=False)
    registration_date_id: Mapped[int] = mapped_column(ForeignKey("dwh.d_calendar.id"), nullable=False)


class UserAnalyticsORM(BaseORM):
    __tablename__ = "f_user_analytics"

    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("dwh.d_user.tg_id"), primary_key=True)
    first_purchases_date_id: Mapped[int] = mapped_column(ForeignKey("dwh.d_calendar.id"), nullable=False)
    last_purchases_date_id: Mapped[int] = mapped_column(ForeignKey("dwh.d_calendar.id"), nullable=False)
    total_purchases: Mapped[int] = mapped_column(BigInteger, nullable=False)
    service_purchases: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sub_purchases: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mtx_purchases: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_revenue: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sub_revenue: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mtx_revenue: Mapped[int] = mapped_column(BigInteger, nullable=False)
    