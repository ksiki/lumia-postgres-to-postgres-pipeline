from typing import Final

from fastapi import APIRouter
from src.api.v1 import users

from src.api.v1.config import PREFIXES


API_ROUTER: Final[APIRouter] = APIRouter()

API_ROUTER.include_router(users.ROUTER, prefix=PREFIXES.users, tags=["Users"])
