from httpx import Response
from typing import Tuple, List
from logging import Logger

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def save_documents(enriched: List[List[str]], responses: Tuple[Response], logger: Logger) -> None:
    if not responses:
        return

    documents = await client.IR.DocsStorage.insert_many(
        [
            {
                'path': response.url.path,
                'words': words
            }
            for response, words in zip(responses, enriched) if response
        ]
    )
    logger.info("Saved %s documents", len(documents.inserted_ids))
