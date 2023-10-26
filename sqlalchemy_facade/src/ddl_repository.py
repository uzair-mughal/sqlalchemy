from typing import List
from sqlalchemy.schema import Sequence
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy_facade.src.declarative_base import Entity
from sqlalchemy.schema import CreateSchema, CreateSequence


class DDLRepository:
    def __init__(self, engine: AsyncEngine):
        self.__engine = engine
        self.__session = None

    async def create_schemas(self, schemas: List[str]):
        """
        Creates schema.
        """

        for schema in schemas:
            async with self.__engine.begin() as connection:
                try:
                    await connection.execute(CreateSchema(name=schema))
                except Exception:
                    pass

    async def create_tables(self):
        """
        Creates tables. Tables of all models that are inherited from Entity will be created.
        """

        async with self.__engine.begin() as connection:
            await connection.run_sync(Entity.metadata.create_all)

    async def drop_tables(self):
        """
        Drop tables. Tables of all models that are inherited from Entity will be dropped.
        """

        async with self.__engine.begin() as connection:
            await connection.run_sync(Entity.metadata.drop_all)

    async def create_sequences(self, sequences: List[str]):
        for sequence in sequences:
            async with self.__engine.begin() as connection:
                schema = sequence.split(".")[0]
                name = sequence.split(".")[1]

                await connection.execute(CreateSequence(Sequence(name=name, start=1, increment=1, schema=schema)))

    async def seed_data(self, table, data: List[dict]):
        async with self.__engine.begin() as connection:
            await connection.execute(table.insert(), data)
