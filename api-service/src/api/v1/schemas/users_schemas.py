from datetime import date
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


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


class UserFilters(BaseModel):
    country: Optional[str] = Field(None)
    city: Optional[str] = Field(None)
    sex: Optional[str] = Field(None, description="Парень/Девушка")
