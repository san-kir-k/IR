import asyncio

from typing import Tuple, Set
from logging import Logger

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def load_urls(logger: Logger, storage) -> Set[str]:
    urls: Set[str] = set()
    try:
        async for record in storage.find():
            urls.add(record['url'])
        logger.info("[%s] : Loaded %s URL", storage.name, len(urls))
    except:
        pass
    return urls


async def dump_urls(urls: Set[str], logger: Logger, storage) -> None:
    insert_tasks = [
        storage.find_one_and_update(
            {'url': url},
            {'$set': {'url': url}},
            upsert=True
        )
        for url in urls
    ]
    results: Tuple = await asyncio.gather(*insert_tasks)
    logger.info("[%s] : Dumped %s URL", storage.name, len(results))


async def load_visited_urls(logger: Logger) -> Set[str]:
    return await load_urls(logger, client.IR.VisitedURLStorage)


async def dump_visited_urls(urls: Set[str], logger: Logger) -> None:
    return await dump_urls(urls, logger, client.IR.VisitedURLStorage)


async def load_pending_urls(logger: Logger) -> Set[str]:
    return await load_urls(logger, client.IR.PendingURLStorage)


async def dump_pending_urls(urls: Set[str], logger: Logger) -> None:
    return await dump_urls(urls, logger, client.IR.PendingURLStorage)
