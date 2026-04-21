from typing import Final

from pydantic_settings import BaseSettings


class Prefixes(BaseSettings):
    api: str
    users: str
    sales: str


PREFIXES: Final[Prefixes] = Prefixes(
    api="/api/v1",
    users="/users",
    sales="/sales"    
)
