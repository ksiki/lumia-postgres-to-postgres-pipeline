from typing import Final
from sqlalchemy.ext.asyncio import (
    AsyncSession, 
    create_async_engine, 
    async_sessionmaker
)

from src.core.config import SETTINGS


class DatabaseHelper:
    def __init__(self, url: str, echo: bool = False):
        self.__engine = create_async_engine(
            url=url, 
            echo=echo
        )
        self.session_factory = async_sessionmaker(
            bind=self.__engine,
            autoflush=False,
            expire_on_commit=False,
        )

    async def session_dependency(self) -> AsyncSession:
        async with self.session_factory() as session:
            yield session


DATABASE_HELPER: Final[DatabaseHelper] = DatabaseHelper(
    url=SETTINGS.db_url,
    echo=SETTINGS.debug
)