from typing import Annotated, Final

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db_helper import database_helper
from src.api.v1.crud import sales_crud as crud
from src.api.v1.schemas.sales_schemas import SalesFilters


router: Final[APIRouter] = APIRouter()


@router.get("/")
async def get_sales(
    filters: Annotated[SalesFilters, Query()],
    session: AsyncSession = Depends(database_helper.session_dependency)
):
    return await crud.get_sales(
        session=session, 
        filters=filters
    )
