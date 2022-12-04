from typing import List, Dict, Set

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def get_words_by_bigrams(bigrams: Set[str]) -> Dict:
    result: Dict = dict()
    for bigram in bigrams:
        async for record in client.IR.BigramStorage.find({'bigram': bigram}):
            words: List[str] = record['words']
            result[bigram] = words
    return result
