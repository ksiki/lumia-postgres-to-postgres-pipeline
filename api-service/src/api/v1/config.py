from typing import Final

from pydantic_settings import BaseSettings


class Prefixes(BaseSettings):
    api_version: str
    users: str
    sales: str


prefixes: Final[Prefixes] = Prefixes(
    api_version="/v1",
    users="/users",
    sales="/sales"    
)
