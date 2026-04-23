from typing import Final
from decouple import config
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    debug: bool
    db_url: str
    x_api_key: str
    api_prefix: str
    maintenance_prefix: str


settings: Final[Settings] = Settings(
    debug=config("DEBUG"),
    db_url=config("DB_URL"),
    x_api_key=config("X_API_KEY"),
    api_prefix="/api",
    maintenance_prefix="/maintenance"
)
