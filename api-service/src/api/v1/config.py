from typing import Final

from src.core.config import Prefixes


class LocalPrefixes(Prefixes):
    users: str
    sales: str


prefixes: Final[LocalPrefixes] = LocalPrefixes(
    api_version="/v1",
    users="/users",
    sales="/sales"    
)
