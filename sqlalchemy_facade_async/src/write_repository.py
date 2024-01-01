import logging
from sqlalchemy.future import select
from sqlalchemy.orm import sessionmaker
from contextlib import asynccontextmanager
from sqlalchemy.sql.expression import delete
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy_facade_async.src.declarative_base import Entity

logger = logging.getLogger(__name__)


class WriteRepository:
    def __init__(self, engine: AsyncEngine):
        self.__engine = engine
        self.__session = None

    @asynccontextmanager
    async def session(self):
        """
        Async context manager, yields the same shared session everytime it is requested.
        Once the call is returned back to this function, the finally block is triggered
        which closes the session.
        """

        try:
            if self.__session is None:
                Session = sessionmaker(bind=self.__engine, autoflush=False, autocommit=False, class_=AsyncSession)

                self.__session = Session()
                logger.debug("Sqlalchemy new session created.")

            yield self.__session
            await self.__session.flush()
        except Exception as e:
            await self.close()
            logger.error(e, exc_info=True)
            raise

    async def start_transaction(self):
        """
        Starts the transaction on the session object.
        """

        async with self.session() as session:
            await session.begin()

    async def close(self):
        if self.__session is not None:
            await self.__session.close()
            logger.debug("Sqlalchemy session closed.")
            self.__session = None

    async def commit(self):
        """
        Commits the transaction and closes the session.
        """

        async with self.session() as session:
            await session.commit()

        await self.close()

    async def rollback(self):
        """
        Rollbacks transaction and closes session.
        """
        async with self.session() as session:
            await session.rollback()

    async def insert(self, row):
        """
        Inserts a single row in database.
        """

        try:
            async with self.session() as session:
                session.add(row)
        except Exception as e:
            await self.close()
            raise

    async def upsert(self, entity: Entity, index_elements: list = ["id"]):
        async with self.session() as session:
            properties = entity.__dict__.copy()
            properties.pop("_sa_instance_state")

            query = (
                insert(entity.__table__)
                .values(properties)
                .on_conflict_do_update(index_elements=index_elements, set_=properties)
            )
            await session.execute(query)
            await session.flush()

    async def insert_ignore(self, entity: Entity):
        async with self.session() as session:
            properties = entity.__dict__.copy()
            properties.pop("_sa_instance_state")

            query = insert(entity.__table__, properties).on_conflict_do_nothing()
            await session.execute(query)
            await session.flush()

    async def insert_ignore_get_id(self, entity: Entity):
        async with self.session() as session:
            properties = entity.__dict__.copy()
            properties.pop("_sa_instance_state")

            query = insert(entity.__table__, properties).on_conflict_do_nothing()
            result = await session.execute(query)
            await session.flush()

            if result.inserted_primary_key:
                return result.inserted_primary_key[0]
            else:
                non_timestamp_properties = {k: v for k, v in properties.items() if type(v) in [str, int, float, bool]}
                result = await session.execute(select(entity.__table__).filter_by(**non_timestamp_properties))
                return result.fetchone()[0]

    async def bulk_insert(self, rows):
        """
        Inserts multiple rows in database.
        """

        try:
            async with self.session() as session:
                session.add_all(rows)
        except Exception as e:
            await self.close()
            raise

    async def update(self, model, filters, properties):
        """
        Updates one or more rows that matches the given filters.
        """

        try:
            async with self.session() as session:
                rows = await session.execute(select(model).filter_by(**filters))

                rows = [row for row, in rows.fetchall()]

                for row in rows:
                    for key, value in properties.items():
                        setattr(row, key, value)
        except Exception as e:
            await self.close()
            raise

    async def delete(self, model, filters):
        """
        Deletes one or more rows that matches the given filters.
        """

        try:
            async with self.session() as session:
                await session.execute(delete(model).filter_by(**filters))
        except Exception as e:
            await self.close()
            raise

    async def execute(self, statement):
        async with self.session() as session:
            await session.execute(statement)

    async def execute_statement(self, statement):
        async with self.session() as session:
            await session.execute(statement)
