import asyncio

from typing import List, Tuple
from logging import Logger
from itertools import chain, tee

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


def get_bigrams(word: str) -> List[str]:
    lhs, rhs = tee(word)
    next(rhs, None)
    return [''.join(p) for p in list(zip(lhs, rhs))]


async def save_words(enriched: List[List[str]], logger: Logger) -> None:
    insert_tasks = [
        client.IR.WordsStorage.find_one_and_update(
            {'word': word},
            {'$set': {'word': word, 'bigrams': get_bigrams(word)}},
            upsert=True
        )
        for word in chain.from_iterable(zip(*enriched))
    ]
    results: Tuple = await asyncio.gather(*insert_tasks)
    logger.info("Inserted %s new words", len(results))
