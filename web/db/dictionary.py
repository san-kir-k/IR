from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def check_if_exists(word: str) -> bool:
    return await client.IR.WordsStorage.count_documents({'word': word}) > 0
