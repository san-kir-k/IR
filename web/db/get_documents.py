from typing import List, Dict

from motor.motor_asyncio import AsyncIOMotorClient
from bson.objectid import ObjectId

client = AsyncIOMotorClient()


async def get_documents(doc_bytes: List) -> List[Dict]:
    doc_ids: List = [ObjectId(bytes(doc_bytes_i)) for doc_bytes_i in doc_bytes]
    result: List[Dict] = []
    for doc_id in doc_ids:
        async for record in client.IR.DocsStorage.find({'_id': doc_id}):
            result.append({"url": "https://en.wikipedia.org" + record["path"], "title": record["title"]})
    return result