from datetime import date

from pydantic import BaseModel, ConfigDict


class User(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    tg_id: int
    sex: str
    city: str
    country: str
    registration_date: date


class UserAnalytics(User):
    first_purchases_date: date
    last_purchases_date: date
    total_purchases: int
    service_purchases: int
    sub_purchases: int
    mtx_purchases: int
    total_revenue: int
    sub_revenue: int
    mtx_revenue: int
