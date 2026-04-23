from typing import Annotated, Final
from fastapi import APIRouter, Depends, Path, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db_helper import database_helper
from src.api.v1.crud import users_crud as crud
from src.api.v1.schemas.users_schemas import (
    User, 
    UserAnalytics, 
    UserFilters
)


router: Final[APIRouter] = APIRouter()


@router.get("/", response_model=list[User])
async def get_users(
    filters: Annotated[UserFilters, Query()],
    session: AsyncSession = Depends(database_helper.session_dependency)
):
    return await crud.get_users(
        session=session, 
        filters=filters
    )


@router.get("/{user_id}/", response_model=UserAnalytics)
async def get_user_analytics(
    user_id: Annotated[int, Path(gt=0)],
    session: AsyncSession = Depends(database_helper.session_dependency)
):
    return await crud.get_user_analytics(
        session=session, 
        user_id=user_id
    )
