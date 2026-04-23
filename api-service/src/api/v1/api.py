from typing import Final

from fastapi import APIRouter

from src.api.v1 import users
from src.api.v1 import sales
from src.api.v1.config import prefixes


api_router: Final[APIRouter] = APIRouter(prefix=prefixes.api_version)

api_router.include_router(users.router, prefix=prefixes.users, tags=[f"{prefixes.api_version}{prefixes.users}"])
api_router.include_router(sales.router, prefix=prefixes.sales, tags=[f"{prefixes.api_version}{prefixes.sales}"])
