from typing import Final
from asyncio import current_task
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker, async_scoped_session 

from src.core.config import SETTINGS


class DatabaseHelper:
    def __init__(self, url: str, echo: bool = False):
        self.__engine = create_async_engine(
            url=url,
            echo=echo
        )
        self.__session_factory = async_sessionmaker(
            bind=self.__engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False
        )

    async def session_dependency(self) -> AsyncSession:
        async with self.__get_scope_session() as connection:
            yield connection
            connection.remove()

    def __get_scope_session(self):
        session = async_scoped_session(
            session_factory=self.__session_factory,
            scopefunc=current_task
        )
        return session 


DATABASE_HELPER: Final[DatabaseHelper] = DatabaseHelper(
    url=SETTINGS.db_url,
    echo=SETTINGS.debug
)