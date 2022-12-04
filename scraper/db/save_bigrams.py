import asyncio

from typing import List

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def save_bigrams() -> None:
    async for record in client.IR.WordsStorage.find():
        word: str = record['word']
        bigrams: List[str] = record['bigrams']

        await asyncio.gather(
            *(
                client.IR.BigramStorage.find_one_and_update(
                    {'bigram': bigram},
                    {'$push': {'words': word}},
                    upsert=True
                )
                for bigram in bigrams
            )
        )
