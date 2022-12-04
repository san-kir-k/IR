from httpx import Response
from typing import Tuple, List
from logging import Logger
from bs4 import BeautifulSoup

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def save_documents(enriched: List[List[str]],
                         responses: Tuple[Response],
                         logger: Logger) -> None:
    if not responses:
        return

    titles: List[str] = []
    for response in responses:
        soup = BeautifulSoup(response.text, features="html.parser")
        title = soup.find(id='firstHeading')
        if title:
            titles.append(title.get_text())

    documents = await client.IR.DocsStorage.insert_many(
        [
            {
                'title': title,
                'path': response.url.path,
                'words': words
            }
            for response, words, title in zip(responses, enriched, titles) if response
        ]
    )

    logger.info("Saved %s documents", len(documents.inserted_ids))
