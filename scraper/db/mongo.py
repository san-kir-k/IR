from httpx import Response
from typing import Tuple
from logging import Logger

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def save_documents(responses: Tuple[Response], logger: Logger) -> None:
    if not responses:
        return

    documents = await client.IR.DocsStorage.insert_many(
        [
            {
                'path': response.url.path,
                'raw_html': response.text
            }
            for response in responses if response
        ]
    )
    logger.info("Saved %s documents", len(documents.inserted_ids))
