from sqlalchemy import select
from sqlalchemy.orm import aliased
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession

from src.user.schemas import User, UserAnalytics
from src.core.models import (
    CalendarORM, 
    CityORM, 
    CountryORM, 
    UserAnalyticsORM, 
    UserORM
)


async def get_users(session: AsyncSession) -> list[User]:
    stmt = (
        select(
            UserORM.tg_id,
            UserORM.sex,
            CityORM.name.label("city"),
            CountryORM.name.label("country"),
            CalendarORM.fact_date.label("registration_date")
        )
        .join(CityORM, UserORM.city_id == CityORM.id)
        .join(CountryORM, CityORM.country_id == CountryORM.id)
        .join(CalendarORM, UserORM.registration_date_id == CalendarORM.id)
    )
    result: Result = await session.execute(stmt)
    users = result.mappings().all()
    return [User.model_validate(user) for user in users]


async def get_user_analytics(session: AsyncSession, user_id: int) -> UserAnalytics | None:
    reg_date = aliased(CalendarORM, name="reg_date")
    first_buy_date = aliased(CalendarORM, name="first_buy")
    last_buy_date = aliased(CalendarORM, name="last_buy")
    
    stmt = (
        select(
            UserORM.tg_id,
            UserORM.sex,
            CityORM.name.label("city"),
            CountryORM.name.label("country"),
            reg_date.fact_date.label("registration_date"),
            first_buy_date.fact_date.label("first_purchases_date"),
            last_buy_date.fact_date.label("last_purchases_date"),
            UserAnalyticsORM.total_purchases,
            UserAnalyticsORM.service_purchases,
            UserAnalyticsORM.sub_purchases,
            UserAnalyticsORM.mtx_purchases,
            UserAnalyticsORM.total_revenue,
            UserAnalyticsORM.sub_revenue,
            UserAnalyticsORM.mtx_revenue
        )
        .join(CityORM, UserORM.city_id == CityORM.id)
        .join(CountryORM, CityORM.country_id == CountryORM.id)
        .join(reg_date, UserORM.registration_date_id == reg_date.id)
        .join(UserAnalyticsORM, UserORM.tg_id == UserAnalyticsORM.user_id)
        .join(first_buy_date, UserAnalyticsORM.first_purchases_date_id == first_buy_date.id)
        .join(last_buy_date, UserAnalyticsORM.last_purchases_date_id == last_buy_date.id)
    ).where((UserORM.tg_id == user_id) & (UserAnalyticsORM.user_id == user_id))
    result: Result = await session.execute(stmt)
    data = result.mappings()

    if analytics := data.one_or_none():
        return UserAnalytics.model_validate(analytics)
    return None