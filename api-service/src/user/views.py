from typing import Annotated, Final
from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import PREFIXES
from src.core.db_helper import DATABASE_HELPER
from src.user import crud
from src.user.schemas import User, UserAnalytics, UserFilters


ROUTER: Final[APIRouter] = APIRouter(
    prefix=PREFIXES.user,
    tags=["User"]
)


@ROUTER.get("/", response_model=list[User])
async def get_users(
    filters: Annotated[UserFilters, Query()],
    session: AsyncSession = Depends(DATABASE_HELPER.session_dependency)
):
    return await crud.get_users(
        session=session, 
        filters=filters
    )


@ROUTER.get("/{user_id}/", response_model=UserAnalytics)
async def get_user_analytics(
    user_id: Annotated[int, Path(gt=0)],
    session: AsyncSession = Depends(DATABASE_HELPER.session_dependency)
):
    return await crud.get_user_analytics(
        session=session, 
        user_id=user_id
    )
