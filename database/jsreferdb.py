"""Referral tracking storage.

Originally used the *synchronous* pymongo driver, which blocked the bot's
event loop on every call (refer checks run inside message handlers). It now
uses the same async motor driver as the rest of the database layer.
"""
from motor.motor_asyncio import AsyncIOMotorClient
from info import DATABASE_URI, DATABASE_NAME
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


class UserTracker:
    def __init__(self, db_uri):
        client = AsyncIOMotorClient(db_uri)
        mydb = client[DATABASE_NAME]
        self.user_collection = mydb["referusers"]
        self.refer_collection = mydb["refers"]

    async def add_user(self, user_id):
        if not await self.is_user_in_list(user_id):
            await self.user_collection.insert_one({'user_id': user_id})

    async def remove_user(self, user_id):
        await self.user_collection.delete_one({'user_id': user_id})

    async def is_user_in_list(self, user_id):
        return bool(await self.user_collection.find_one({'user_id': user_id}))

    async def add_refer_points(self, user_id: int, points: int):
        await self.refer_collection.update_one(
            {'user_id': user_id},
            {'$set': {'points': points}},
            upsert=True
        )

    async def get_refer_points(self, user_id: int):
        user = await self.refer_collection.find_one({'user_id': user_id})
        return user.get('points') if user else 0


referdb = UserTracker(DATABASE_URI)
