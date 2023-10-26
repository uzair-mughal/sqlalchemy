import logging
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getlogger(__name__)


class ReadRepository:
    def __init__(self, engine: AsyncEngine):
        self.__engine = engine
        self.__session = None

    @asynccontextmanager
    async def session(self):
        """
        Async context manager, yields a new session everytime it is requested.
        Once the call is returned back to this function, the finally block is triggered
        which closes the session.
        """

        try:
            Session = sessionmaker(bind=self.__engine, autoflush=False, autocommit=False, class_=AsyncSession)

            self.__session = Session()

            logger.debug("Sqlalchemy new session created.")
            yield self.__session
        except Exception as e:
            logger.error(e, exc_info=True)
            await self.close()
            raise
        finally:
            await self.close()

    async def close(self):
        if self.__session is not None:
            await self.__session.close()
            logger.debug("Sqlalchemy session closed.")
            self.__session = None

    async def select(self, model, filters: dict):
        """
        Selects a single row from the table specified by the model while
        applying the given filters.
        """

        async with self.session() as session:
            rows = await session.execute(select(model).filter_by(**filters))

            try:
                return rows.fetchone()[0]
            except Exception:
                return None

    async def select_all(self, model, filters: dict):
        """
        Selects multiple rows from the table specified by the model while
        applying the given filters.
        """

        async with self.session() as session:
            rows = await session.execute(select(model).filter_by(**filters))

            return [row for row, in rows.fetchall()]

    async def execute(self, statement):
        async with self.session() as session:
            rows = await session.execute(statement)
            return rows.fetchall()

    async def execute_in_batch(self, statement, batch_size: int = 1000):
        async with self.session() as session:
            result_proxy = await session.execute(statement)
            while True:
                batch = result_proxy.fetchmany(batch_size)
                if not batch:
                    break
                yield batch
