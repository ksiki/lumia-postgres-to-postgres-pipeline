from typing import Final
from decouple import config
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    debug: bool
    db_url: str


SETTINGS: Final[Settings] = Settings(
    debug=config("DEBUG"),
    db_url=config("DB_URL")
)
