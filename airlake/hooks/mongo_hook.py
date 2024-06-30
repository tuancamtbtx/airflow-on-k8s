from typing import List, Tuple
from airflow.providers.mongo.hooks.mongo import MongoHook as MongoHookBase
import motor.motor_asyncio
from pymongo.collection import Collection


class MongoHook(MongoHookBase):
    def __init__(self, conn_id: str = ..., *args, **kwargs) -> None:
        super().__init__(conn_id, *args, **kwargs)
        self.async_client = motor.motor_asyncio.AsyncIOMotorClient(self.uri)

    async def bulk_write_async(
        self,
        mongo_db: str,
        mongo_collection: str,
        requests: list,
        ordered: bool = True,
    ):
        if len(requests) == 0:
            return

        db = self.async_client[mongo_db]
        collection: Collection = db[mongo_collection]
        return await collection.bulk_write(requests, ordered)

    async def insert_many_async(
        self,
        mongo_db: str,
        mongo_collection: str,
        documents: list,
        ordered: bool = True,
    ):
        if len(documents) == 0:
            return

        db = self.async_client[mongo_db]
        collection: Collection = db[mongo_collection]
        return await collection.insert_many(documents=documents, ordered=ordered)

    def count(
        self,
        mongo_db: str,
        mongo_collection: str,
        filter: dict,
    ):
        collection: Collection = self.get_collection(mongo_collection=mongo_collection, mongo_db=mongo_db)
        return collection.count_documents(filter=filter)

    def find(
        self,
        mongo_db: str,
        mongo_collection: str,
        filter: dict,
        limit: int = 0,
        offset: int = 0,
    ):
        collection: Collection = self.get_collection(mongo_collection=mongo_collection, mongo_db=mongo_db)
        return collection.find(filter).skip(offset).limit(limit)

    def create_index(
        self,
        mongo_db: str,
        mongo_collection: str,
        name: str,
        keys: List[Tuple[str]],
        background: bool,
        unique: bool
    ):
        collection: Collection = self.get_collection(mongo_collection=mongo_collection, mongo_db=mongo_db)
        return collection.create_index(keys=keys, name=name, background=background, unique=unique)

    def delete(
        self,
        mongo_db: str,
        mongo_collection: str,
        filter: dict,
    ):
        collection: Collection = self.get_collection(mongo_collection=mongo_collection, mongo_db=mongo_db)
        return collection.delete_many(filter=filter)
